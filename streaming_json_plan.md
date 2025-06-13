# JSONストリーミング処理実装計画

## 概要

現在のJSONLoaderは、ファイル全体をメモリに読み込んでから処理する方式を採用しています。大きなJSONファイル（数GB）を処理する際にメモリ不足が発生する可能性があるため、ストリーミング処理による改善を計画します。

## 現在の問題点

### メモリ使用量の問題
```go
// 現在の実装 - 全体をメモリに読み込み
fileData, err := os.ReadFile(l.FilePath)
var jsonData []map[string]interface{}
err = json.Unmarshal(fileData, &jsonData)
```

### 問題の具体例
- 1GBのJSONファイル → 最低1GB RAM使用
- 複数の並列処理 → メモリ使用量が倍増
- OOMキラーによるプロセス終了リスク

## 解決アプローチ

### 1. ストリーミングJSONパーサーの実装

#### Option A: 標準ライブラリベース
```go
type StreamingJSONLoader struct {
    FilePath string
    BufferSize int // デフォルト: 64KB
}

func (l *StreamingJSONLoader) Load(columns []string) ([]DataRecord, error) {
    file, err := os.Open(l.FilePath)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    decoder := json.NewDecoder(file)
    
    // 配列の開始を確認
    if !expectDelim(decoder, '[') {
        return nil, fmt.Errorf("expected JSON array")
    }
    
    var records []DataRecord
    var actualColumns []string
    isFirstRecord := true
    
    for decoder.More() {
        var jsonObj map[string]interface{}
        if err := decoder.Decode(&jsonObj); err != nil {
            return nil, fmt.Errorf("error decoding JSON object: %w", err)
        }
        
        // 最初のレコードでカラム検出
        if isFirstRecord && len(columns) == 0 {
            actualColumns = extractAndSortKeys(jsonObj)
            isFirstRecord = false
        } else if isFirstRecord {
            actualColumns = columns
            isFirstRecord = false
        }
        
        record := convertObjectToRecord(jsonObj, actualColumns)
        records = append(records, record)
    }
    
    return records, nil
}
```

#### Option B: 外部ライブラリ（jsoniter）
```go
import "github.com/json-iterator/go"

func (l *StreamingJSONLoader) LoadWithJsoniter(columns []string) ([]DataRecord, error) {
    file, err := os.Open(l.FilePath)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    iter := jsoniter.Parse(jsoniter.ConfigDefault, file, 4096)
    // ストリーミング処理実装
}
```

### 2. メモリ効率化戦略

#### バッチ処理アプローチ
```go
type BatchingJSONLoader struct {
    FilePath   string
    BatchSize  int    // デフォルト: 1000レコード
    OnBatch    func([]DataRecord) error // バッチ処理コールバック
}

func (l *BatchingJSONLoader) ProcessInBatches(columns []string) error {
    // 1000件ずつ処理してメモリを開放
    decoder := json.NewDecoder(file)
    batch := make([]DataRecord, 0, l.BatchSize)
    
    for decoder.More() {
        // レコード読み込み
        if len(batch) >= l.BatchSize {
            if err := l.OnBatch(batch); err != nil {
                return err
            }
            batch = batch[:0] // スライスをリセット
        }
    }
}
```

### 3. アーキテクチャ設計

#### インターフェース拡張
```go
type Loader interface {
    Load(columns []string) ([]DataRecord, error)
}

type StreamingLoader interface {
    Loader
    LoadInBatches(columns []string, batchSize int, callback func([]DataRecord) error) error
    EstimateMemoryUsage() (int64, error)
}

type JSONLoader struct {
    FilePath        string
    StreamingMode   bool // falseで従来動作、trueでストリーミング
    BatchSize       int  // ストリーミング時のバッチサイズ
    MaxMemoryUsage  int64 // 最大メモリ使用量制限
}
```

## 実装計画

### Phase 1: 基本ストリーミング実装（2-3日）

1. **StreamingJSONLoader構造体の実装**
   - `json.NewDecoder`を使用したストリーミング読み込み
   - 基本的なエラーハンドリング
   - カラム自動検出の維持

2. **既存インターフェースとの互換性**
   - `Load()`メソッドの実装（小さなファイルは従来通り）
   - 閾値ベースの自動切り替え（例：100MB以上でストリーミング）

3. **基本テスト**
   - 小さなJSONファイルでの動作確認
   - メモリ使用量の測定

### Phase 2: パフォーマンス最適化（3-4日）

1. **バッチ処理の実装**
   - 設定可能なバッチサイズ
   - メモリ使用量の監視
   - GC最適化

2. **大容量ファイルテスト**
   - 1GB以上のテストファイル生成
   - メモリ使用量ベンチマーク
   - パフォーマンス比較

3. **設定オプション追加**
   ```yaml
   # mydatasyncer.yml
   loader:
     json:
       streaming_threshold: 100MB  # この値以上でストリーミング
       batch_size: 1000           # バッチサイズ
       max_memory: 512MB          # メモリ制限
   ```

### Phase 3: 本格運用対応（2-3日）

1. **エラー処理の強化**
   - 部分的な失敗からの回復
   - プログレス表示
   - キャンセル処理

2. **並列処理サポート**
   - ワーカープールパターン
   - チャネルベースの処理

3. **統合テスト**
   - E2Eテストでの大容量ファイル処理
   - CI/CDでのメモリ制限テスト

## メモリ使用量の改善見込み

### 現在の実装
```
1GB JSONファイル → 1GB+ RAM使用
10GB JSONファイル → 処理不可（OOM）
```

### ストリーミング実装後
```
1GB JSONファイル → 64MB RAM使用（バッチサイズ1000件の場合）
10GB JSONファイル → 64MB RAM使用（処理時間は増加）
```

## リスクと対策

### リスク1: パフォーマンスの低下
- **対策**: 小さなファイルは従来方式を維持
- **対策**: ベンチマークテストによる閾値調整

### リスク2: 複雑性の増加
- **対策**: 既存APIとの互換性維持
- **対策**: 設定による動作切り替え

### リスク3: JSON構造の制限
- **対策**: 配列以外の構造のエラーハンドリング
- **対策**: 不正なJSONの早期検出

## 実装優先度

### 高優先度
1. 基本ストリーミング機能
2. メモリ使用量制限
3. 自動切り替え機能

### 中優先度
1. バッチ処理最適化
2. プログレス表示
3. 並列処理

### 低優先度
1. 外部ライブラリ統合
2. 詳細な設定オプション
3. パフォーマンスダッシュボード

## 成功指標

1. **メモリ効率**: 1GB+ ファイルを 100MB 以下で処理
2. **互換性**: 既存テストの100%パス
3. **パフォーマンス**: 小さなファイル（<10MB）の処理時間劣化 <10%
4. **安定性**: 24時間連続処理でのメモリリーク皆無

## 実装開始タイミング

この機能は以下の条件が揃った時点で開始することを推奨：

1. 現在のJSON型変換改善が完了
2. ユーザーから大容量ファイル処理の要望
3. 開発リソースの確保（1-2週間）

現時点では「将来の拡張機能」として位置づけ、実際の需要に応じて実装を検討するのが適切と考えます。