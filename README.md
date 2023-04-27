# Concurrency demo scripts
A couple of scripts demonstrating naive vs concurrent approach to I/O + CPU bound work.

## Description
First script is `naive.py`, which will compute the task sequentially.
Contrary, `main.py` will use thread / process pools to deal with I/O / CPU bound load, respectively.
    
## Prerequisites
* Python 3.11

## Manual

`python main.py -h`

    usage: main.py [-h] [-z ZIP_COUNT] [-c XML_COUNT] [-n XML_NESTING] [--view]

    Run a script demonstrating I/O & CPU-bounded process execution time.
    
    options:
      -h, --help      show this help message and exit
      -z ZIP_COUNT    How many zip files will be created (default: 50). I/O & CPU-bounded param
      -c XML_COUNT    How many XML files will be created per zip file (default: 100). I/O-bounded param
      -n XML_NESTING  How nested each XML will be (default: random [1..10]). CPU-bounded param
      --view          Stop script execution to view files in the output directory (default: False)
    

## Examples

`python naive.py`

    Execution time for store_zip function is: 12.591873 s
    Execution time for store_csv function is: 8.529399 s

`python main.py -z 50 -c 20 -n 1000 --view`

    Execution time for store_zip function is: 3.437540 s
    Execution time for store_csv function is: 4.693735 s
    You can view the output directory at C:\Users\skse\AppData\Local\Temp\tmpccqrmyq2
    Press enter to delete temp files...

Examples above were calculated using

	Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz
	Cores: 4
	Logical processors: 8


## Tests

Just a couple of sanity checks within the app. User can also review the output dir for created files.
