import pandas as pd
import numpy as np
import torch

CATEGORIES = [
    'Unknown',
    'Bank',
    'Bills',
    'Charity',
    'Coffee',
    'Drinks',
    'Eating Out',
    'Gift',
    'Health',
    'Leisure',
    'Lunch',
    'Pets',
    'Shopping',
    'Supermarket',
    'Takeaway',
    'Transport'
]


class TransactionDataset(object):
    def __init__(self, args, encoder):
        self.vocab_size = args.vocab_size
        self.seq_len = args.sequence_length

        self.encoder = encoder

    def encode_dataset(self, sequences):
        x = list(self.encoder.transform(sequences))
        seq_lengths = [len(y) for y in x]

        # Pad with zeros
        x = [y + [0] * (self.seq_len - len(y)) for y in x]
        return np.array(x), seq_lengths

    def generate(self, filename):
        # This dataset is small so we can get away with not lazy-loading
        dataset = pd.read_csv(filename)
        data, lens = self.encode_dataset(dataset['description'])

        def get_idx(x):
            try:
                return CATEGORIES.index(x)
            except ValueError:
                return 0

        raw_labels = dataset["WideCategory"].apply(get_idx)

        input_ids = torch.tensor(data, dtype=torch.long)
        input_lengths = torch.tensor(lens, dtype=torch.long)
        labels = torch.tensor(raw_labels, dtype=torch.long)

        return input_ids, input_lengths, labels
