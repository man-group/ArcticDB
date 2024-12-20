#include <vector>
#include <iostream>

void f() {
    int* use_after_delete = new int(10);
    delete use_after_delete;
    std::cout<<*use_after_delete;

    std::vector<int> use_after_move(10);
    std::vector<int> moved = std::move(use_after_move);
    std::cout<<use_after_move[0];
}
