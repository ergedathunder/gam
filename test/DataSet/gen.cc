#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <map>

using namespace std;

map<int , map<int, int> > Q;

int main() {

    freopen ("2.txt", "w", stdout);

    srand(time(NULL));
    int n = 50000, m = 1000000;
    for (int i = 1; i <= m; ++i) {
        int x, y;
        while (1) {
            x = rand() % n + 1;
            y = rand() % n + 1;
            if (Q[x][y] || Q[y][x]) continue;
            printf ("%d %d\n", x, y);
            break;
        }
        Q[x][y] = 1;
    }
    // int inter_val = n / 100;
    // for (int i = 1; i <= 50000; ++i) {
    //     int Right = ( (i % inter_val) + 1);
    //     if (Right == i) Right++;
    //     for (int j = 1; j <= 100; ++j) {
    //         if (Right > n) break;
    //         ++ m;
    //         printf ("%d %d\n", Right, i);
    //         Right += inter_val;
    //     }
    // }

    // int interval_j = 64; // 64 * 8 = BLOCK_SIZE;
    // int interval_i = 64; //n / num_threads
    // for (int i = 2; i <= n; i += interval_i) {
    //     int Left = 1;
    //     while (Left <= n) {
    //         if (Left == i) {
    //             Left ++;
    //             continue;
    //         }
    //         printf ("%d %d\n", Left, i);
    //         Left += interval_j;
    //     }
    // }

    return 0;
}