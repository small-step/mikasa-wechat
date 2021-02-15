#include <iostream>
#include <Shlwapi.h>
#pragma comment(lib, "Shlwapi.lib")

#include "injector.h"

void show_help()
{
    std::cout << "[Usage] injector.exe [process name] [dll path]\n";
}

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        show_help();
        return 0;
    }
    if (!PathFileExists(argv[2]))
    {
        std::cout << "[Injector] DLL path does NOT exist: " << argv[2] << "\n";
        return 0;
    }

    std::cout << "[Injector] Process name: " << argv[1] << "\n";
    std::cout << "[Injector] DLL path: " << argv[2] << "\n";
    int res = inject_dll(argv[1], argv[2]);

    return res;
}