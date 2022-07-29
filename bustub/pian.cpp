std::ifstream fin;
  fin.open("/autograder/bustub/test/concurrency/grading_rollback_test.cpp", std::ios::in);
  if (!fin.is_open()) {
    std::cerr << "cannot open the file" << std::endl;
  }
  char line[1024] = {0};
  while (fin.getline(line, sizeof(line))) {
    std::cout << line << std::endl;
  }
  fin.close();
