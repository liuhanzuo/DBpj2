## BabyDB

This is the codebase for Database System course, IIIS, Tsinghua University in 2025.

You should start from the branch `main`, and when each project posted, this branch will be updated. So before you start your next project, confirm your code is up to date.

### Cloning this Repository

1. Go [here](https://github.com/new) to create a new empty (without README.md) repository under your account. Pick a name (e.g. `babydb-private`) and select **Private** for the repository visibility level.
2. Create a bare clone of the repository:

```
$ git clone --bare https://github.com/hehezhou/babydb.git
```

3. Mirror-push to the new repository:

```
$ cd babydb.git
$ git push https://github.com/USERNAME/babydb-private.git main
```

And remove the filefolder:

```
$ cd ..
$ rm babydb.git -rf
```

4. Clone your private repository to your machine:

```
$ git clone https://github.com/USERNAME/babydb-private.git
$ cd babydb-private
```

5. Add the codebase BabyDB repository as a second remote, which allows you to retrieve changes from the codebase repository and merge them into your repository:

```
$ git remote add codebase https://github.com/hehezhou/babydb.git
```

You can verify that the remote was added with the following command:

```
$ git remote -v
codebase        https://github.com/hehezhou/babydb.git (fetch)
codebase        https://github.com/hehezhou/babydb.git (push)
origin ...
```

You can now pull in changes from the codebase repository as needed with:

```
$ git pull codebase main
```

We suggest you working on your projects in separate branches. If you do not understand how Git branches work, [learn how](https://git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging).

### Build

**We recommend you run this code on Linux.**

Before start building, make sure you have `make`, `cmake` and C++ compiler (`g++` or `clang++`) on your machine and they are added into your environment path, which means you can directly use them on your shell.

Then run the following commands to build the system:

```
$ mkdir build
$ cd build
$ cmake ..
$ make -j`nproc`
```

If you want to compile the system in release mode, pass in the following flag to cmake:

```
$ cmake -DCMAKE_BUILD_TYPE=Release ..
$ make -j`nproc`
```

Otherwise by default the system will be compiled in debug mode. We recommend you to develop and debug on debug mode, but test performance on release mode.

By default, debug mode will use address sanitizer. If you want to use other sanitizers like undefined sanitizer, pass in the following flag:

```
$ cmake -DBABYDB_SANITIZER=undefined ..
$ make -j`nproc`
```

Then you can run the tests:

```
$ make check-tests
```

or get more details:

```
$ make check-tests-details
```

You may fail on some tests. If their name are all start with `Project`, don't worry, they will be passed if you correctly complete your project. But if you fail on other tests, please check your steps, and contact with the TA.

### Project 1

You should only modify `src/storage/art.cpp` and `src/include/storage/art.hpp`, and pass all `Project1ArtIndexMVCC` tests if you complete the whole project.
