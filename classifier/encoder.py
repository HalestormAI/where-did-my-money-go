from bpe import Encoder
import pandas as pd


def create(self, vocab_size, dataset_filename, vocab_filename):
    dataset = pd.read_csv(dataset_filename)
    corpus = dataset['description'].values
    encoder = Encoder(vocab_size)
    encoder.fit(corpus)
    encoder.save(vocab_filename)
    return encoder


def load(self, vocab_filename):
    return Encoder.load(vocab_filename)
