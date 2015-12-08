*************************
** Introduction        **
*************************
Project 2 for CSCE 608 Database Systems, Fall 2015.
Github Repository located at: https://github.com/jasmeet13n/tinysql-db

*************************
** Authors             **
*************************
Deep Desai (dkdnco @ tamu . edu)
Jasmeet Singh (jasmeet13n @ tamu . edu)


*************************
** System Requirements **
*************************
G++ compiler supporting C++11 standard. The system is tested by the authors on a Macbook Pro with OS X El Capitan, C++ version 4.2.1 and Apple LLVM version 7.0.0 (clang-700.1.76)
Use constexpr instead of const in StorageManger Disk.h file as constexpr is required as of C++11 standard. Does the following changes to lines 29, 30, 31 of Disk.h in StorageManger library:
static constexpr double avg_seek_time=6.46;
static constexpr double avg_rotation_latency=4.17;
static constexpr double avg_transfer_time_per_block=0.20 * 320;


*************************
** Build Project       **
*************************
In a terminal window with the directory changed to project directory, run the following command:
> make all
This will produce the executable file in a.out


*************************
** Run Project         **
*************************
There can be two ways to run the project, first to input queries using a file and other way is to manually type in the queries. The two methods are described below:
1) Input from file:
> ./a.out < TinySQL_linux.txt

2) Manual Input: (type in 'exit' to stop the program)
> ./a.out
> #input queries here ...
> exit
