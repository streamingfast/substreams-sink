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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBlockBuffer(tt.fields.size)
			b.blocks = tt.fields.blocks
			if err := b.AddBlock(tt.args.block); (err != nil) != tt.wantErr {
				t.Errorf("BlockBuffer.AddBlock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBlockBuffer_GetBlocks(t *testing.T) {
	type fields struct {
		size   int
		blocks []*pbsubstreams.BlockScopedData
	}
	tests := []struct {
		name    string
		fields  fields
		want    []*pbsubstreams.BlockScopedData
		wantErr bool
	}{
		{
			name: "get blocks",
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
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 4, "4a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 5, "5a"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBlockBuffer(tt.fields.size)
			b.blocks = tt.fields.blocks
			got, err := b.GetBlocks()
			if (err != nil) != tt.wantErr {
				t.Errorf("BlockBuffer.GetBlocks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("BlockBuffer.GetBlocks() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewBlockBuffer_HandleUndo(t *testing.T) {
	type fields struct {
		size   int
		blocks []*pbsubstreams.BlockScopedData
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
			name: "undo new",
			fields: fields{
				size: 10,
				blocks: []*pbsubstreams.BlockScopedData{
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "2a"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 3, "3x"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 4, "4y"),
					testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 4, "5z"),
				},
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_UNDO, 2, "pp"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBlockBuffer(tt.fields.size)
			b.blocks = tt.fields.blocks
			b.handleUndo(tt.args.block)
			if len(b.blocks) != len(tt.want) {
				t.Errorf("BlockBuffer.HandleUndo() = %v, want %v", b.blocks, tt.want)
			}
		})
	}
}

func TestBlockBuffer_HandleNew(t *testing.T) {
	type fields struct {
		size   int
		blocks []*pbsubstreams.BlockScopedData
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
			},
			args: args{
				block: testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "pp"),
			},
			want: []*pbsubstreams.BlockScopedData{
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 1, "1a"),
				testBlockScopedData(pbsubstreams.ForkStep_STEP_NEW, 2, "pp"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBlockBuffer(tt.fields.size)
			b.blocks = tt.fields.blocks
			b.handleNew(tt.args.block)
			if len(b.blocks) != len(tt.want) {
				t.Errorf("BlockBuffer.HandleNew() = %v, want %v", b.blocks, tt.want)
			}
		})
	}
}
