import csv
import os
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, Float, MetaData

class CSVDumpWriter:

    def __init__(self, output_path, plain_header=False, separate_header=True):
        self._output_path = output_path
        self._plain_header = plain_header
        self._separate_header = separate_header

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        self._write_header('blocks', ['hash:ID(Block)', 'height:int', 'timestamp:int'])
        self._write_header('transactions', ['txid:ID(Transaction)', 'coinbase:boolean'])
        self._write_header('outputs', ['txid_n:ID(Output)', 'n:int', 'value:double', 'type'])
        self._write_header('addresses', ['address:ID(Address)'])
        self._write_header('rel_block_tx', ['hash:START_ID(Block)', 'txid:END_ID(Transaction)'])
        self._write_header('rel_tx_output',
                           ['txid:START_ID(Transaction)', 'txid_n:END_ID(Output)'])
        self._write_header('rel_input', ['txid:END_ID(Transaction)', 'txid_n:START_ID(Output)'])
        self._write_header('rel_output_address',
                           ['txid_n:START_ID(Output)', 'address:END_ID(Address)'])

    def __enter__(self):
        self._blocks_file = open(self._get_path('blocks'), 'a')
        self._transactions_file = open(self._get_path('transactions'), 'a')
        self._outputs_file = open(self._get_path('outputs'), 'a')
        self._addresses_file = open(self._get_path('addresses'), 'a')
        self._rel_block_tx_file = open(self._get_path('rel_block_tx'), 'a')
        self._rel_tx_output_file = open(self._get_path('rel_tx_output'), 'a')
        self._rel_input_file = open(self._get_path('rel_input'), 'a')
        self._rel_output_address_file = open(self._get_path('rel_output_address'), 'a')

        self._block_writer = csv.writer(self._blocks_file)
        self._transaction_writer = csv.writer(self._transactions_file)
        self._output_writer = csv.writer(self._outputs_file)
        self._address_writer = csv.writer(self._addresses_file)
        self._rel_block_tx_writer = csv.writer(self._rel_block_tx_file)
        self._rel_tx_output_writer = csv.writer(self._rel_tx_output_file)
        self._rel_input_writer = csv.writer(self._rel_input_file)
        self._rel_output_address_writer = csv.writer(self._rel_output_address_file)
        return self

    def __exit__(self, type, value, traceback):
        self._blocks_file.close()
        self._transactions_file.close()
        self._outputs_file.close()
        self._addresses_file.close()
        self._rel_block_tx_file.close()
        self._rel_tx_output_file.close()
        self._rel_input_file.close()
        self._rel_output_address_file.close()

    def _write_header(self, filename, row):
        if self._separate_header:
            filename += '_header'
        with open(self._get_path(filename), 'w') as f:
            writer = csv.writer(f)
            if self._plain_header:
                header = [entry.partition(':')[0] for entry in row]
            else:
                header = row
            writer.writerow(header)

    def _get_path(self, filename):
        return os.path.join(self._output_path, filename + '.csv')

    def write(self, block):
        def a_b(a, b):
            return '{}_{}'.format(a, b)

        self._block_writer.writerow([block.hash, block.height, block.timestamp])
        for tx in block.transactions:
            self._transaction_writer.writerow([tx.txid, tx.is_coinbase()])
            self._rel_block_tx_writer.writerow([block.hash, tx.txid])
            if not tx.is_coinbase():
                for input in tx.inputs:
                    self._rel_input_writer.writerow(
                        [tx.txid,
                         a_b(input.output_reference['txid'], input.output_reference['vout'])])
            for output in tx.outputs:
                self._output_writer.writerow([a_b(tx.txid, output.index), output.index,
                                              output.value, output.type])
                self._rel_tx_output_writer.writerow([tx.txid, a_b(tx.txid, output.index)])
                for address in output.addresses:
                    self._address_writer.writerow([address])
                    self._rel_output_address_writer.writerow([a_b(tx.txid, output.index), address])

class DBDumpWriter:

    def __init__(self, engine):
        self._engine = engine
        self._metadata = MetaData()

        self._blocks = Table('blocks', self._metadata,
            Column('hash', String(64), primary_key=True),
            Column('height', Integer),
            Column('timestamp', Integer),
        )

        self._transactions = Table('transactions', self._metadata,
            Column('txid', String(64), primary_key=True),
            Column('coinbase', Integer)
        )

        self._outputs = Table('outputs', self._metadata,
            Column('txid_n', String(64)),
            Column('n', Integer),
            Column('value', Float),
            Column('type', String(64))
        )

        self._addresses = Table('addresses', self._metadata,
            Column('address', String(64), primary_key=True)
        )

        self._rel_block_tx = Table('rel_block_tx', self._metadata,
            Column('hash', String(64)),
            Column('txid', String(64)),
        )

        self._rel_tx_output = Table('rel_tx_output', self._metadata,
            Column('txid', String(64)),
            Column('txid_n', String(64)),
        )

        self._rel_input = Table('rel_input', self._metadata,
            Column('txid', String(64)),
            Column('txid_n', String(64)),
        )

        self._rel_output_address = Table('rel_output_address', self._metadata,
            Column('txid_n', String(64)),
            Column('address', String(64)),
        )

        self._metadata.create_all(self._engine)

    def __enter__(self):
        # Do I need to do anything?
        pass

    def __exit__(self, type, value, traceback):
        # Do I need to do anything?
        pass

    def write(self, block):
        def a_b(a, b):
            return '{}_{}'.format(a, b)

        self._engine.execute(self._blocks.insert(), hash=block.hash, height=block.height, timestamp=block.timestamp)
        for tx in block.transactions:
            self._engine.execute(self._transactions.insert(), txid=tx.txid, coinbase=tx.is_coinbase())
            self._engine.execute(self._rel_block_tx.insert(), hash=block.hash, txid=tx.txid)

            if not tx.is_coinbase():
                for input in tx.inputs:
                    self._engine.execute(self._rel_input.insert(), txid=tx.txid, txid_n=a_b(input.output_reference['txid'], input.output_reference['vout']))

            for output in tx.outputs:
                self._engine.execute(self._outputs.insert(), txid_n=a_b(tx.txid, output.index), n=output.index, value=output.value, type=output.type)                
                self._engine.execute(self._rel_tx_output.insert(), txid=tx.txid, txid_n=a_b(tx.txid, output.index))
                for address in output.addresses:
                    self._engine.execute(self._addresses.insert(), address=address)
                    self._engine.execute(self._rel_output_address.insert(), txid_n=a_b(tx.txid, output.index), address=address)
