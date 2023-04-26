# Concurrency demo scripts
A couple of scripts demonstrating naive vs concurrent approach to I/O + CPU bound work.

## Description
First script is `naive.py`, which will compute the work sequentially.
Contrary, `main.py` will use thread / process pools to deal with I/O / CPU bound load respectively.
    
## Prerequisites
* Python 3.11

## Usage

`python naive.py`

    Execution time for store_xml function is: 11.501882 s
    Execution time for store_csv function is: 17.712826 s

`python main.py --view -c 1000 -n 1000`

    Execution time for store_xml function is: 3.681963 s
    Execution time for store_csv function is: 6.534934 s
    You can view the output directory at C:\Users\skse\AppData\Local\Temp\tmpon0cg79q
    Press enter to delete temp files...

Examples above were calculated using

	Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz
	Cores: 4
	Logical processors: 8


## Tests

Just a couple of sanity checks within the app. User can also review the output dir for created files.
