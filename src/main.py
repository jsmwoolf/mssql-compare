import argparse
import os.path
from parser.parser import convertFileToMetaData

parser = argparse.ArgumentParser(description="The Microsoft SQL Server comparison tool")
parser.add_argument('-s', '--source', type=str, help="The original SQL file/database.", required=True)
parser.add_argument('-u', '--updated', type=str, help="The updated SQL file/database.", required=True)

def main():
    args = parser.parse_args()
    print(args.source)
    if os.path.isfile(args.source):
        for meta in convertFileToMetaData(args.source):
            print(meta)


if __name__ == '__main__':
    main()