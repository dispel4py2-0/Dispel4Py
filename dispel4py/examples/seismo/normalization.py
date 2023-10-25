"""
Created on Mon Apr 07 09:23:16 2014

@author: abell5
"""
import copy

from numpy import (
    absolute,
    amax,
    arange,
    convolve,
    floor,
    logical_and,
    sign,
    sum,
    true_divide,
    zeros,
)
from scipy.signal import triang


def onebit_norm(stream):
    stream2 = copy.deepcopy(stream)

    for trace in arange(len(stream2)):
        data = stream2[trace].data
        data = sign(data)
        stream2[trace].data = data

    return stream2


def mean_norm(stream, N):
    stream2 = copy.deepcopy(stream)

    for trace in arange(len(stream2)):
        data = stream2[trace].data

        w = zeros(len(data))
        naux = zeros(len(data))

        for n in arange(len(data)):
            if n < N:
                tw = absolute(data[0: n + N])
            elif logical_and(n >= N, n < (len(data) - N)):
                tw = absolute(data[n - N: n + N])
            elif n >= (len(data) - N):
                tw = absolute(data[n - N: len(data)])

            w[n] = true_divide(1, 2 * N + 1) * (sum(tw))

        naux = true_divide(data, w)
        stream2[trace].data = naux

    return stream2


def gain_norm(stream, N):
    stream2 = copy.deepcopy(stream)

    for trace in arange(len(stream2)):
        data = stream2[trace].data

        dt = 1.0 / (stream2[trace].stats.sampling_rate)
        L = floor(N / dt + 1.0 / 2.0)
        h = triang(2.0 * L + 1.0)

        e = data**2.0

        rms = convolve(e, h, "same") ** 0.5
        epsilon = 1.0e-12 * amax(rms)
        op = rms / (rms**2 + epsilon)
        dout = data * op

        stream2[trace].data = dout

    return stream2
