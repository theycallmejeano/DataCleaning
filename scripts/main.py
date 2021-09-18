import sys
import getopt
from cleaning_script import HealthCleaner

# write json file
def main(argv):
    inputfile = ''
    outputfile = ''

    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        sys.stderr.write('main.py -i <inputfile> -o <outputfile>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            sys.stdout.write('main.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg

    if outputfile == "":
        outputfile = inputfile

    file = HealthCleaner(inputfile)
    
    # write dict to json-file
    csv_file = HealthCleaner(outputfile)
    csv_file.writeContent(file)


if __name__ == '__main__':
    main(sys.argv[1:])