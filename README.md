# spiral-challenge

This project aims to build a API to register and execute workflows.

## Description: 

The project keeps persistance of workflow and operator state by creating a one-to-one relationship between workflow object and file with unique filename. The workflow object contains a dag attribute that is built upon this workflow's operators and dependency relationships. To achieve better performance, the project implement a least frequently used cache to decrease the number of reading access to disk. 

## Test
create your own test or run `python test.py` in the corresponding directory
>>>>>>> 05a69430cf88d3b5dddbaac367c99cc2cafb48f5
