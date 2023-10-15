"""
Created on Mon Apr 07 09:30:46 2014

@author: abell5
"""

import copy

from numpy import abs, arange, conjugate, multiply, real, sqrt
from obspy.signal.util import nextpow2
from scipy.fftpack import fft, ifft


def spectralwhitening(stream):
    """
    Apply spectral whitening to data.
    Data is divided by its smoothed (Default: None) amplitude spectrum.
    """
    stream2 = copy.deepcopy(stream)

    for trace in arange(len(stream2)):
        data = stream2[trace].data

        n = len(data)
        nfft = nextpow2(n)

        spec = fft(data, nfft)
        spec_ampl = sqrt(abs(multiply(spec, conjugate(spec))))

        spec /= spec_ampl  # Do we need to do some smoothing here?
        ret = real(ifft(spec, nfft)[:n])

        stream2[trace].data = ret

    return stream2


def spectralwhitening_smooth(stream, N):
    """
    Apply spectral whitening to data.
    Data is divided by its smoothed (Default: None) amplitude spectrum.
    """
    stream2 = copy.deepcopy(stream)

    for trace in arange(len(stream2)):
        data = stream2[trace].data

        n = len(data)
        nfft = nextpow2(n)

        spec = fft(data, nfft)
        spec_ampl = sqrt(abs(multiply(spec, conjugate(spec))))

        spec_ampl = smooth(spec_ampl, N)

        spec /= spec_ampl  # Do we need to do some smoothing here?
        ret = real(ifft(spec, nfft)[:n])

        stream2[trace].data = ret

    return stream2


def smooth(spec_ampl, N):
    # ....
    return spec_ampl
