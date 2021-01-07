import csv
import logging
import os
from pathlib import Path
import datetime
from kbc.env_handler import KBCEnvHandler

APP_VERSION = "0.0.4"

KEY_PRINT_ROWS = 'print_rows'
MANDATORY_PARS = [KEY_PRINT_ROWS]


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)

        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params
        print_rows = params.get(KEY_PRINT_ROWS)

        input_tables = self.get_input_tables_definitions()
        first_input_table = input_tables[0]

        logging.info('Running...')

        output_file_name = 'output.csv'
        output_file = os.path.join(self.tables_out_path, output_file_name)

        with open(first_input_table.full_path, 'r') as input, open(output_file, 'w+', newline='') as out:
            reader = csv.DictReader(input)
            new_columns = reader.fieldnames
            # append row number col
            new_columns.append('row_number')
            writer = csv.DictWriter(out, fieldnames=new_columns, lineterminator='\n', delimiter=',')
            writer.writeheader()
            for index, l in enumerate(reader):
                # print line
                if print_rows:
                    logging.info(f'Printing line {index}: {l}')
                # add row number
                l['row_number'] = index
                writer.writerow(l)

        self.configuration.write_table_manifest(
            file_name=output_file,
            primary_key=['row_number'],
            incremental=True
        )

        state = self.get_state_file()

        last_update = None

        if 'last_update' in state:
            last_update = state['last_update']

        logging.info(f'Last update: {last_update}')

        state['last_update'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.write_state_file(state)


if __name__ == "__main__":
    try:
        (Component()).run()
    except Exception as e:
        logging.exception(e)
        exit(1)
