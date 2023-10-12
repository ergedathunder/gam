#include <bits/stdc++.h>
#include <unistd.h>

using namespace std;

int main() {
    char buffer[100]; // 用于存储路径的缓冲区
    if (getcwd(buffer, sizeof(buffer)) != nullptr) {
        cout << "当前位置: " << buffer << std::endl;
    } else {
        cerr << "无法获取当前位置" << std::endl;
    }
    FILE * fp = fopen ("DataSet/1.txt", "r");
	if (fp == NULL) {
		printf("File exception!\n");
		return false; 
	}

	printf ("start read_file\n");
    fflush(stdout);
    fclose(fp);

    return 0;
}