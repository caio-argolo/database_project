#!/home/caio/anaconda3/bin/python3

import argparse
import logging
import os
import sys
from db_util import db_commands as db
import util.logger_manip as utl

# Initialize log object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
utl.initialize_logger(os.getcwd(), logger)


def main():
	parser = argparse.ArgumentParser(description="A Tool manipulate a sqlite DB")

	subparsers = parser.add_subparsers(title='actions',
									   description='valid actions',
									   help='Use sqlite-python.py {action} -h for help with each action',
									   dest='command'
									   )

	parser_index = subparsers.add_parser('createdb', help='Create database and tables')

	parser_index.add_argument("--db", dest='db', default=None, action="store", help="The DB name",
						required=True)



	parser_insert = subparsers.add_parser('insert', help='Insert data on tables')

	parser_insert.add_argument("--file",  default=None, action="store", help="TSV file with the data to be inserted",
						required=True)

	parser_insert.add_argument("--db", default=None, action="store", help="The DB name",
						required=True)



	parser_update = subparsers.add_parser('update', help='Update field assay or donor in a db')

	parser_update.add_argument("--db", default=None, action="store", help="The DB name",
						required=True)

	parser_update.add_argument("--assay", default=None, action="store", help="Name of the assay",
						required=False)

	parser_update.add_argument("--new_assay", default=None, action="store", help="New name for the assay",
						required=False)

	parser_update.add_argument("--donor", default=None, action="store", help="Name of the donor",
						required=False)

	parser_update.add_argument("--new_donor", default=None, action="store", help="New name for the donor",
						required=False)


	parser_select = subparsers.add_parser('select', help='Select  fields from the db')

	parser_select.add_argument("--db", default=None, action="store", help="The DB name",
						required=True)

	parser_select.add_argument("--assay_track", action="store", help="Select track info of a given assay", 
						required=False)

	parser_select.add_argument("--cell_type", action="store_true", help="Select all Cell types",
						required=False)

	parser_select.add_argument("--track_name", action="store", help="Select \"track_name\" with a assay_track_name",
						required=False)

	parser_select.add_argument("--assay_cell", action="store", help="Select \"cell_type\" of a given assay",
						required=False)



	parser_delete = subparsers.add_parser('delete', help='delete rows from the db')

	parser_delete.add_argument("--track_name", default=None, action="store", help="Delete rows related to this track name ",
						required=False)

	parser_delete.add_argument("--db", default=None, action="store", help="The DB name",
						required=True)



	args = parser.parse_args()
	# print(args)

	# sys.exit()
	conn = db.connect_db(args.db, logger)

	if args.command == "createdb":

		db.create_table(conn, logger)


	elif args.command == "insert":
		list_of_data = []

		with open(args.file, 'r') as f:
			for line in f:

				# reset dictionary
				line_dict = dict()

				# Skip empty lines
				if line.startswith(",,"):
					continue

				if not line.strip():
					continue

				# split line
				values = line.strip().split(',')

				# put each field in a dict
				line_dict['cell_type_category'] = values[0]
				line_dict['cell_type'] = values[1]
				line_dict['cell_type_track_name'] = values[2]
				line_dict['cell_type_short'] = values[3]
				line_dict['assay_category'] = values[4]
				line_dict['assay'] = values[5]
				line_dict['assay_track_name'] = values[6]
				line_dict['assay_short'] = values[7]
				line_dict['donor'] = values[8]
				line_dict['time_point'] = values[9]
				line_dict['view'] = values[10]
				line_dict['track_name'] = values[11]
				line_dict['track_type'] = values[12]
				line_dict['track_density'] = values[13]
				line_dict['provider_institution'] = values[14]
				line_dict['source_server'] = values[15]
				line_dict['source_path_to_file'] = values[16]
				line_dict['server'] = values[17]
				line_dict['path_to_file'] = values[18]
				line_dict['new_file_name'] = values[19]

				#append the dict to a list
				list_of_data.append(line_dict)

		db.insert_data(conn, list_of_data, logger)


	elif args.command == "update" and args.new_assay is not None:
		db.update_assay(conn, args.assay, args.new_assay, logger)



	elif args.command == "update" and args.new_donor is not None:
		db.update_donor(conn, args.donor, args.new_donor, logger)



	elif args.command == "select" and args.cell_type is not False:
		all_celltypes = db.select_celltype(conn, logger)

		for cell_type in all_celltypes:
			print(cell_type[0])



	elif args.command == "select" and args.assay_track is not None:
		all_tracks = db.select_track_from_assay(conn, args.assay_track, logger)

		print("\n| Track_name\t| Track_type\t| Track_Density")
		for tracks in all_tracks:
			print("|","\t| ".join(tracks))



	elif args.command == "select" and args.track_name is not None:
		all_track_names = db.select_track_names(conn, args.track_name, logger)

		for track_name in all_track_names:
			print(track_name[0])

	elif args.command == "delete":
		db.delete_trackname(conn, args.track_name, logger)



	elif args.command == "select" and args.assay_cell is not None:
		all_celltypes = db.select_cell_from_assay(conn, args.assay_cell, logger)

		for cell_type in all_celltypes:
			print(cell_type[0])



	elif args.command == "delete" and args.track_name is not None:
		db.delete_trackname(conn, args.track_name, logger)

if __name__ == '__main__':
	main()