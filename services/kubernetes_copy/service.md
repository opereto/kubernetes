This service parses the pytest results directory (provided as parser_directory_path input property) and generates the Opereto test results directory (provided as listener_directory_path input property) structure as required by Opereto standard test results listener service.
The parser runs in a loop until it finds the junit/xunit file in the parser directory path specified, parses it and creates the new results directory for the listener.

#### Service success criteria
Success by default unless terminated explicitly with different status.

#### Dependencies
* Opereto worker virtual environment