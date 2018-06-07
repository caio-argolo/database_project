#!/home/caio/anaconda3/bin/python3

import sqlite3


def connect_db(db_name, logger):
	try:
		conn = sqlite3.connect(db_name + '.db')
		logger.info(f'Connetion stablished with DB: {db_name}.db')

		return conn


	except sqlite3.OperationalError:
		logger.error(f'Could not connect with {db_name}.db. Make sure the DB name is right')



def create_table(conn, logger):
	c = conn.cursor()

	try:
		c.execute('CREATE TABLE IF NOT EXISTS chip_seq('
					'cell_type_category TEXT NOT NULL, '
					'cell_type TEXT NOT NULL, '
					'cell_type_track_name TEXT NOT NULL, '
					'cell_type_short TEXT NOT NULL, '
					'assay_category TEXT NOT NULL, '
					'assay TEXT NOT NULL, '
					'assay_track_name TEXT NOT NULL, '
					'assay_short TEXT NOT NULL, '
					'donor TEXT NOT NULL, '
					'time_point TEXT NOT NULL, '
					'view TEXT NOT NULL, '
					'track_name TEXT NOT NULL, '
					'track_type TEXT NOT NULL, '
					'track_density TEXT NOT NULL, '
					'provider_institution TEXT NOT NULL, '
					'source_server TEXT NOT NULL, '
					'source_path_to_file TEXT NOT NULL, '
					'server TEXT NOT NULL, '
					'path_to_file TEXT NOT NULL, '
					'new_file_name TEXT NOT NULL);')

		logger.info('Table chip_seq was created')

	except sqlite3.OperationalError:
		logger.error('Table chip_seq could not be created')



def insert_data(conn, list_of_data, logger):
	c = conn.cursor()

	try:
		with conn:
			for data in list_of_data:
					c.execute("INSERT INTO chip_seq VALUES(:cell_type_category, :cell_type, :cell_type_track_name, :cell_type_short, :assay_category, :assay, :assay_track_name, :assay_short, :donor, :time_point, :view, :track_name, :track_type, :track_density, :provider_institution, :source_server, :source_path_to_file, :server, :path_to_file, :new_file_name)", data)
			logger.info('Data was inserted on the DB')

	except sqlite3.OperationalError:
		logger.error('Data could not be inserted')



def update_assay(conn, assay, new_assay, logger):

	c = conn.cursor()

	try:
		with conn:
			c.execute("UPDATE chip_seq SET assay = :new_assay  WHERE assay = :assay", {'new_assay': new_assay, 'assay': assay})
			logger.info(f'Assay: {assay} was updated to new_assay: {new_assay}')

	except sqlite3.OperationalError:
		logger.error(f'COULD NOT UPDATE Assay:{assay} to new assay: {new_assay}')



def update_donor(conn, donor, new_donor, logger):

	c = conn.cursor()

	try:
		with conn:
			c.execute("UPDATE chip_seq SET donor = :new_donor  WHERE donor = :donor", {'new_donor': new_donor, 'donor': donor})
			logger.info(f'Donor: {donor} was updated to New_donor: {new_donor}')

	except sqlite3.OperationalError:
		logger.error(f'COULD NOT UPDATE Donor:{donor} to New_donor: {new_donor}')



def select_celltype(conn, logger):

	c = conn.cursor()

	try:
		with conn:
			c.execute("SELECT DISTINCT cell_type FROM chip_seq")
			all_cell_types = c.fetchall()

			logger.info(f'Selected cell types')
			return all_cell_types

	except sqlite3.OperationalError:
		logger.error(f'Could not Select cell types. Check if the table exists.')



def select_track_from_assay(conn, assay, logger):
	c = conn.cursor()

	try:
		with conn:
			c.execute("SELECT track_name, track_type, track_density FROM chip_seq WHERE assay = :assay", {"assay": assay})
			track_info = c.fetchall()

			logger.info(f'Selected track info from assay: {assay}')

			return track_info

	except sqlite3.OperationalError:
		logger.error(f'Could not Select chip_seq track info. Check if the table exists.')

def select_track_names(conn, assay_track_name, logger):
	c = conn.cursor()

	try:
		with conn:
			c.execute("SELECT track_name FROM chip_seq WHERE assay_track_name = :assay_track_name", {"assay_track_name": assay_track_name})
			track_name = c.fetchall()

			logger.info(f'Selected track name from assay_track_name: {assay_track_name}')

			return track_name

	except sqlite3.OperationalError:
		logger.error(f'Could not Select chip_seq track_name. Check if the table exists.')



def select_cell_from_assay(conn, assay, logger):
	c = conn.cursor()

	try:
		with conn:
			c.execute("SELECT cell_type FROM chip_seq WHERE assay = :assay", {"assay": assay})
			cell = c.fetchall()

			logger.info(f'Selected cell type from assay: {assay}')

			return cell

	except sqlite3.OperationalError:
		logger.error(f'Could not Select chip_seq cell type. Check if the table exists.')



def delete_trackname(conn, track_name, logger):
	c = conn.cursor()

	try:
		with conn:
			c.execute("DELETE FROM chip_seq WHERE track_name = :track_name", {"track_name": track_name})

			logger.info(f'Rows where track_name is: "{track_name}" were deleted')

	except sqlite3.OperationalError:
		logger.error(f'Could not delete {track_name}')