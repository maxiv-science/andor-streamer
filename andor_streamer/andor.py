import sys
import time
import numpy as np
from cffi import FFI

ffi = FFI()
ffi.cdef('''
    typedef int AT_H;
    typedef int AT_BOOL;
    typedef long long AT_64;
    typedef unsigned char AT_U8;
    typedef wchar_t AT_WC;

    int AT_InitialiseLibrary();
    int AT_FinaliseLibrary();

    int AT_Open(int CameraIndex, AT_H *Hndl);
    int AT_Close(AT_H Hndl);
    
    int AT_IsImplemented(AT_H Hndl, const AT_WC* Feature, AT_BOOL* Implemented);
    int AT_IsReadable(AT_H Hndl, const AT_WC* Feature, AT_BOOL* Readable);
    int AT_IsWritable(AT_H Hndl, const AT_WC* Feature, AT_BOOL* Writable);
    int AT_IsReadOnly(AT_H Hndl, const AT_WC* Feature, AT_BOOL* ReadOnly);

    int AT_SetInt(AT_H Hndl, const AT_WC* Feature, AT_64 Value);
    int AT_GetInt(AT_H Hndl, const AT_WC* Feature, AT_64* Value);
    int AT_GetIntMax(AT_H Hndl, const AT_WC* Feature, AT_64* MaxValue);
    int AT_GetIntMin(AT_H Hndl, const AT_WC* Feature, AT_64* MinValue);

    int AT_SetFloat(AT_H Hndl, const AT_WC* Feature, double Value);
    int AT_GetFloat(AT_H Hndl, const AT_WC* Feature, double* Value);
    int AT_GetFloatMax(AT_H Hndl, const AT_WC* Feature, double* MaxValue);
    int AT_GetFloatMin(AT_H Hndl, const AT_WC* Feature, double* MinValue);

    int AT_SetBool(AT_H Hndl, const AT_WC* Feature, AT_BOOL Value);
    int AT_GetBool(AT_H Hndl, const AT_WC* Feature, AT_BOOL* Value);
    
    int AT_SetEnumIndex(AT_H Hndl, const AT_WC* Feature, int Value);
    int AT_SetEnumString(AT_H Hndl, const AT_WC* Feature, const AT_WC* String);
    int AT_GetEnumIndex(AT_H Hndl, const AT_WC* Feature, int* Value);
    int AT_GetEnumCount(AT_H Hndl,const  AT_WC* Feature, int* Count);
    int AT_IsEnumIndexAvailable(AT_H Hndl, const AT_WC* Feature, int Index, AT_BOOL* Available);
    int AT_IsEnumIndexImplemented(AT_H Hndl, const AT_WC* Feature, int Index, AT_BOOL* Implemented);
    int AT_GetEnumStringByIndex(AT_H Hndl, const AT_WC* Feature, int Index, AT_WC* String, int StringLength);

    int AT_Command(AT_H Hndl, const AT_WC* Feature);

    int AT_SetString(AT_H Hndl, const AT_WC* Feature, const AT_WC* String);
    int AT_GetString(AT_H Hndl, const AT_WC* Feature, AT_WC* String, int StringLength);
    int AT_GetStringMaxLength(AT_H Hndl, const AT_WC* Feature, int* MaxStringLength);

    int AT_QueueBuffer(AT_H Hndl, AT_U8* Ptr, int PtrSize);
    int AT_WaitBuffer(AT_H Hndl, AT_U8** Ptr, int* PtrSize, unsigned int Timeout);
    int AT_Flush(AT_H Hndl);
    ''')

AT_SUCCESS = 0

errors = {
    1: 'AT_ERR_NOTINITIALISED',
    2: 'AT_ERR_NOTIMPLEMENTED',
    3: 'AT_ERR_READONLY',
    4: 'AT_ERR_NOTREADABLE',
    5: 'AT_ERR_NOTWRITABLE',
    6: 'AT_ERR_OUTOFRANGE',
    13: 'AT_ERR_TIMEDOUT',
    19: 'AT_ERR_STRINGNOTIMPLEMENTED'
}

sdk = ffi.dlopen('libatcore.so')
AT_HANDLE_SYSTEM = 1

def check_error(ret):
    if ret != AT_SUCCESS:
        msg = errors.get(ret, '')
        print('Error in Andor SDK! error code: %d, %s' %(ret, msg))
        
def get_int(handle, command):
    result = ffi.new('AT_64*')
    check_error(sdk.AT_GetInt(handle, command, result))
    return result[0]

def get_float(handle, command):
    result = ffi.new('double*')
    check_error(sdk.AT_GetFloat(handle, command, result))
    return result[0]

def get_bool(handle, command):
    result = ffi.new('AT_BOOL*')
    check_error(sdk.AT_GetBool(handle, command, result))
    return result[0]

def get_string(handle, command):
    result_length = 128
    result = ffi.new('AT_WC [%s]' % result_length)
    check_error(sdk.AT_GetString(handle, command, result, result_length))
    return ffi.string(result)
 
def get_enum_string_by_index(handle, command, index):
    result_length = 128
    result = ffi.new('AT_WC [%s]' % result_length)
    check_error(sdk.AT_GetEnumStringByIndex(handle, command, index, result, result_length))
    return ffi.string(result)

def set_enum_string(handle, command, item):
    check_error(sdk.AT_SetEnumString(handle, command, item))
    
def get_enum_index(handle, command):
    result = ffi.new('int*')
    check_error(sdk.AT_GetEnumIndex(handle, command, result))
    return result[0]
    
def get_enum_string(handle, command):
    index = get_enum_index(handle, command)
    return get_enum_string_by_index(handle, command, index)
    
def get_enum_count(handle, command):
    result = ffi.new('int*')
    check_error(sdk.AT_GetEnumCount(handle, command, result))
    return result[0]

def get_enum_string_options(handle, command) :
    count = get_enum_count(handle, command)
    options = []
    for i in range(0, count):
        options.append(get_enum_string_by_index(handle, command, i))          
    return options 

def get_float_min(handle, feature):
    result = ffi.new('double*')
    check_error(sdk.AT_GetFloatMin(handle, feature, result))
    return result[0]

def get_float_max(handle, feature):
    result = ffi.new('double*')
    check_error(sdk.AT_GetFloatMax(handle, feature, result))
    return result[0]

def is_implemented(handle, feature):
    result = ffi.new('AT_BOOL*')
    check_error(sdk.AT_IsImplemented(handle, feature, result))
    return result[0]

def wait_buffer(handle, timeout=0):
    buf_ptr = ffi.new('AT_U8**')
    buffer_size = ffi.new('int*')
    ret = sdk.AT_WaitBuffer(handle, buf_ptr, buffer_size, timeout)
    if ret == AT_SUCCESS:
        return (buf_ptr[0], buffer_size[0])
    else:
        print(ret)
        return None
