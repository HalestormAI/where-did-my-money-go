import torch.nn as nn
from torch.nn.utils.rnn import pack_padded_sequence, pad_packed_sequence


class DescriptionClassifier(nn.Module):
    def __init__(self, args, num_classes):
        super(DescriptionClassifier, self).__init__()

        self.hidden_size = args.hidden_size
        self.embedding = nn.Embedding(args.vocab_size, args.embedding_depth)
        self.lstm = nn.LSTM(input_size=args.embedding_depth,
                            hidden_size=self.hidden_size,
                            num_layers=1,
                            batch_first=True,
                            bidirectional=True)
        self.dropout = nn.Dropout(p=args.dropout_rate)

        # 2 * hidden size to account for the fwd and reverse view of the sequence
        self.projection = nn.Linear(2 * self.hidden_size, num_classes)

    def forward(self, text, seq_len):
        embed = self.embedding(text)

        packed_input = pack_padded_sequence(embed, seq_len, batch_first=True, enforce_sorted=False)
        packed_output, _ = self.lstm(packed_input)
        output, _ = pad_packed_sequence(packed_output, batch_first=True)

        out_forward = output[range(len(output)), seq_len - 1, :self.hidden_size]
        out_reverse = output[:, 0, self.hidden_size:]
        out_combined = torch.cat((out_forward, out_reverse), 1)

        out_combined = self.dropout(out_combined)

        # Project the forward and reverse feature map into the class list
        output = self.projection(out_combined)
        return output
