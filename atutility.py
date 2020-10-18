import sys
import time
import numpy as np
from cffi import FFI

ffi = FFI()
ffi.cdef('''
    typedef long long AT_64;
    typedef unsigned char AT_U8;
    typedef wchar_t AT_WC; 
    
    int AT_ConvertBuffer(AT_U8* inputBuffer,
                         AT_U8* outputBuffer,
                         AT_64 width,
                         AT_64 height,
                         AT_64 stride,
                         const AT_WC* inputPixelEncoding,
                         const AT_WC* outputPixelEncoding);
                         
    int AT_InitialiseUtilityLibrary();
    '''
)

sdk = ffi.dlopen('libatutility.so')
