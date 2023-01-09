package sink

import (
	"testing"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

func testBlockScopedData(step pbsubstreams.ForkStep, num uint64, id string) *pbsubstreams.BlockScopedData {
	return &pbsubstreams.BlockScopedData{
		Clock: &pbsubstreams.Clock{
			Number: num,
			Id:     id,
		},
		Step:   step,
		Cursor: "",
	}
}

func TestBlockBuffer_AddBlock(t *testing.T) {
	type fields struct {
		size   int
		blocks []*pbsubstreams.BlockScopedData
	}
	type args struct {
		block *pbsubstreams.BlockScopedData
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "add new block",
			fields: fields{
				size: 10,
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
			},
			wantErr: false,
		},
		{
			name: "add undo block",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_UNDO, 2, "2a"),
			},
			wantErr: false,
		},
		{
			name: "add irr block",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
			},
			wantErr: false,
		},
		{
			name: "add unknown step block",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_UNKNOWN, 2, "2a"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBlockDataBuffer(tt.fields.size)
			b.data = tt.fields.blocks
			if err := b.AddBlockData(tt.args.block); (err != nil) != tt.wantErr {
				t.Errorf("blockDataBuffer.AddBlock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBlockBuffer_GetBlocks(t *testing.T) {
	type fields struct {
		size   int
		irrIdx int
		blocks []*pbsubstreams.BlockScopedData
	}
	tests := []struct {
		name    string
		fields  fields
		want    []*pbsubstreams.BlockScopedData
		wantErr bool
	}{
		{
			name: "get data",
			fields: fields{
				size: 3,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 3, "3a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 4, "4a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 5, "5a"),
				},
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
			},
			wantErr: false,
		},
		{
			name: "get data with irr",
			fields: fields{
				size:   3,
				irrIdx: 4,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 2, "2a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 4, "4a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 5, "5a"),
				},
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 2, "2a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 4, "4a"),
			},
			wantErr: false,
		},
		{
			name: "get data with irr",
			fields: fields{
				size:   10,
				irrIdx: 4,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 2, "2a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 4, "4a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 5, "5a"),
				},
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 2, "2a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 4, "4a"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBlockDataBuffer(tt.fields.size)
			b.irrIdx = tt.fields.irrIdx
			b.data = tt.fields.blocks
			got, err := b.GetBlockData()
			if (err != nil) != tt.wantErr {
				t.Errorf("blockDataBuffer.GetBlocks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("blockDataBuffer.GetBlocks() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewBlockBuffer_HandleUndo(t *testing.T) {
	type fields struct {
		size              int
		lastBlockReturned uint64
		blocks            []*pbsubstreams.BlockScopedData
	}
	type args struct {
		block *pbsubstreams.BlockScopedData
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*pbsubstreams.BlockScopedData
		wantErr bool
	}{
		{
			name: "undo one",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 3, "3x"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 4, "4y"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 5, "5z"),
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_UNDO, 5, "5z"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 3, "3x"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 4, "4y"),
			},
		},
		{
			name: "undo many",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 3, "3x"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 4, "4y"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 5, "5z"),
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_UNDO, 2, "pp"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
			},
		},
		{
			name: "undo empty",
			fields: fields{
				size:   10,
				blocks: []*pbsubstreams.BlockScopedData{},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_UNDO, 1, "lol"),
			},
			want: []*pbsubstreams.BlockScopedData{},
		},
		{
			name: "undo too far",
			fields: fields{
				size:              10,
				lastBlockReturned: 9,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 10, "10a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 20, "20a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 30, "30x"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 40, "40y"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 50, "50z"),
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_UNDO, 5, "5a"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 10, "10a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 20, "20a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 30, "30x"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 40, "40y"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 50, "50z"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBlockDataBuffer(tt.fields.size)
			b.data = tt.fields.blocks
			b.lastBlockReturned = tt.fields.lastBlockReturned
			err := b.handleUndo(tt.args.block)
			if (err != nil) != tt.wantErr {
				t.Errorf("blockDataBuffer.handleUndo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(b.data) != len(tt.want) {
				t.Errorf("blockDataBuffer.HandleUndo() = %v, want %v", b.data, tt.want)
			}
		})
	}
}

func TestBlockBuffer_HandleNew(t *testing.T) {
	type fields struct {
		size   int
		blocks []*pbsubstreams.BlockScopedData
		index  map[bufferKey]bool
	}
	type args struct {
		block *pbsubstreams.BlockScopedData
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*pbsubstreams.BlockScopedData
	}{
		{
			name: "new",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				},
				index: map[bufferKey]bool{},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "pp"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "pp"),
			},
		},
		{
			name: "add already existing block",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				},
				index: map[bufferKey]bool{
					newBufferKey(1, "1a", pbsubstreams.ForkStep_STEP_NEW): true,
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBlockDataBuffer(tt.fields.size)
			b.data = tt.fields.blocks
			b.index = tt.fields.index
			b.handleNew(tt.args.block)
			if len(b.data) != len(tt.want) {
				t.Errorf("blockDataBuffer.HandleNew() = %v, want %v", b.data, tt.want)
			}
		})
	}
}

func TestBlockDataBuffer_HandleIrreversible(t *testing.T) {
	type fields struct {
		size   int
		blocks []*pbsubstreams.BlockScopedData
		index  map[bufferKey]bool
	}
	type args struct {
		block *pbsubstreams.BlockScopedData
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantIrrIdx int
		want       []*pbsubstreams.BlockScopedData
	}{
		{
			name: "irr simple",
			fields: fields{
				size:   10,
				blocks: []*pbsubstreams.BlockScopedData{},
				index:  map[bufferKey]bool{},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
			},
			wantIrrIdx: 1,
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
			},
		},
		{
			name: "irr mixed",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 2, "2a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
				},
				index: map[bufferKey]bool{},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 4, "4a"),
			},
			wantIrrIdx: 4,
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 2, "2a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 3, "3a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 4, "4a"),
			},
		},
		{
			name: "add already existing block",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
				},
				index: map[bufferKey]bool{
					newBufferKey(1, "1a", pbsubstreams.ForkStep_STEP_IRREVERSIBLE): true,
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_IRREVERSIBLE, 1, "1a"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBlockDataBuffer(tt.fields.size)
			b.data = tt.fields.blocks
			b.index = tt.fields.index
			b.handleIrreversible(tt.args.block)
			if len(b.data) != len(tt.want) {
				t.Errorf("blockDataBuffer.HandleIrreversible() = %v, want %v", b.data, tt.want)
			}
			if b.irrIdx != tt.wantIrrIdx {
				t.Errorf("blockDataBuffer.HandleIrreversible() = %v, want %v", b.irrIdx, tt.wantIrrIdx)
			}
		})
	}
}
