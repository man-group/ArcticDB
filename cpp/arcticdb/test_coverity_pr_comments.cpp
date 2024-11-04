#include <vector>
#include <iostream>

void my_function_with_errors() {
	{
		std::vector<int> a = { 1, 2, 3 };
		auto b = std::move(a);

		// Use after move
		std::cout << a.size();
	}

	{
		int* b = new int(5);
		delete b;

		// Use after free
		std::cout << *b;
	}

	{
		int* ptr = nullptr;
		std::cout << *ptr;
	}
}