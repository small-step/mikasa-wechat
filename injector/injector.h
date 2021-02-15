#pragma once

#ifdef __cplusplus
extern "C" {
#endif

enum ResultCode
{
    Success             = 0,
    NoProcess           = 1,
    OpenFailed          = 2,
    AllocFailed         = 3,
    WriteFailed         = 4,
    CreateThreadFailed  = 5
};

__declspec(dllexport) int _cdecl inject_dll(const char* process_name, const char* dll_path, bool ignore_case = true);

#ifdef __cplusplus
}
#endif
