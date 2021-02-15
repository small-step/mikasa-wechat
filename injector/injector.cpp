#include "injector.h"

#include <iostream>
#include <algorithm>
#include <string>
#include <ctype.h>
#include <Windows.h>
#include <tlhelp32.h>
#include <Shlwapi.h>
#pragma comment(lib, "Shlwapi.lib")

int get_pid_from_name(const char* process_name, bool ignore_case)
{
	HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
	if (snapshot == INVALID_HANDLE_VALUE)
	{
		return 0;
	}

	PROCESSENTRY32 structprocsnapshot = { 0 };
	structprocsnapshot.dwSize = sizeof(PROCESSENTRY32);
	if (!Process32First(snapshot, &structprocsnapshot))
	{
		return 0;
	}
	
	do
	{
		std::string exe_name = structprocsnapshot.szExeFile;
		std::string pro_name = process_name;
		if (ignore_case)
		{
			std::transform(pro_name.begin(), pro_name.end(), pro_name.begin(),
				[](unsigned char c) { return std::tolower(c); });
			std::transform(exe_name.begin(), exe_name.end(), exe_name.begin(),
				[](unsigned char c) { return std::tolower(c); });
		}
		if (exe_name == pro_name)
		{
			CloseHandle(snapshot);
			return structprocsnapshot.th32ProcessID;
		}
	} while (Process32Next(snapshot, &structprocsnapshot));

	CloseHandle(snapshot);
	std::cout << "[Injector] Unable to find Process: " << process_name << "\n";
	return 0;

}

int inject_dll(const char* process_name, const char* dll_path, bool ignore_case)
{
	int pid = get_pid_from_name(process_name, ignore_case);
	if (!pid)
	{
		return NoProcess;
	}

	size_t dll_size = strlen(dll_path) + 1;
	HANDLE proc = OpenProcess(PROCESS_ALL_ACCESS, FALSE, pid);
	if (!proc)
	{
		std::cout << "[Injector] Fail to open target process!\n";
		return OpenFailed;
	}

	LPVOID mem_addr = VirtualAllocEx(proc, NULL, dll_size, MEM_COMMIT, PAGE_EXECUTE_READWRITE);
	if (!mem_addr)
	{
		std::cout << "[Injector] Fail to allocate memory in Target Process!\n";
		return AllocFailed;
	}

	int write_success = WriteProcessMemory(proc, mem_addr, dll_path, dll_size, 0);
	if (!write_success)
	{
		std::cout << "[Injector] Fail to write in Target Process memory!\n";
		return WriteFailed;
	}

	DWORD dword;
	LPTHREAD_START_ROUTINE library = (LPTHREAD_START_ROUTINE)GetProcAddress(LoadLibrary("kernel32"), "LoadLibraryA");
	HANDLE handle = CreateRemoteThread(proc, NULL, 0, library, mem_addr, 0, &dword);
	if (!handle)
	{
		std::cout << "[Injector] Fail to create Remote Thread!\n";
		return CreateThreadFailed;
	}

	std::cout << "[Injector] DLL Successfully Injected!\n";
	return Success;
}