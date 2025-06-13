# 親子関係テーブル同期機能 - 詳細作業計画

## 📋 概要と課題分析

### 現在の状況
- **単一テーブル同期のみサポート**: 1ファイル → 1テーブルの単純な構造
- **参照整合性未考慮**: 外部キー制約やテーブル間依存関係の処理なし
- **同期順序の問題**: 複数テーブル間での適切な処理順序が未定義

### 主要な技術的課題
1. **参照整合性の維持**: 親レコード不存在時の子レコード挿入防止
2. **同期順序の決定**: 依存関係に基づく処理順序の自動決定
3. **設定の複雑化**: 複数テーブル・関係性定義の管理
4. **エラーハンドリング**: 参照整合性エラー・部分失敗時の適切な処理
5. **パフォーマンス**: 大量データでの効率的な依存関係解決

## 🎯 Phase 1: アーキテクチャ設計・設定拡張 (1-2週間)

### 1.1 多テーブル設定構造の設計
**目標**: 複数テーブルと依存関係を定義できる設定形式の確立

```yaml
# 新しい設定ファイル形式例
db:
  dsn: "user:password@tcp(127.0.0.1:3306)/testdb?parseTime=true"

# 複数テーブル定義
tables:
  - name: "users"
    filePath: "./data/users.csv"
    primaryKey: "user_id"
    syncMode: "diff"
    deleteNotInFile: true
    timestampColumns: ["created_at", "updated_at"]
    immutableColumns: ["created_at"]
    
  - name: "orders"
    filePath: "./data/orders.csv"
    primaryKey: "order_id"
    syncMode: "diff"
    deleteNotInFile: true
    dependencies:
      - table: "users"
        foreignKey: "user_id"
        referencedKey: "user_id"
        
  - name: "order_items"
    filePath: "./data/order_items.csv"
    primaryKey: "item_id"
    syncMode: "diff"
    dependencies:
      - table: "orders"
        foreignKey: "order_id"
        referencedKey: "order_id"
```

**実装タスク**:
- [ ] `TableConfig`構造体の設計・実装
- [ ] `DependencyConfig`構造体の実装
- [ ] `MultiTableConfig`構造体の実装
- [ ] 設定ファイルパーサーの拡張
- [ ] 後方互換性の維持（単一テーブル設定サポート継続）

### 1.2 依存関係グラフ構造の設計
**目標**: テーブル間の依存関係を効率的に管理する構造の確立

```go
type DependencyGraph struct {
    nodes map[string]*TableNode
    edges map[string][]string
}

type TableNode struct {
    TableName   string
    Config      TableConfig
    InDegree    int
    OutDegree   int
    Dependencies []Dependency
}

type Dependency struct {
    ParentTable    string
    ChildTable     string
    ForeignKey     string
    ReferencedKey  string
}
```

**実装タスク**:
- [ ] 依存関係グラフ構造の実装
- [ ] グラフ構築アルゴリズムの実装
- [ ] 循環依存検出機能の実装
- [ ] グラフ可視化・デバッグ機能の実装

## 🔧 Phase 2: 核心機能実装 (2-3週間)

### 2.1 トポロジカルソート実装
**目標**: 依存関係に基づく適切な処理順序の自動決定

```go
type SyncOrderResolver struct {
    graph *DependencyGraph
}

func (r *SyncOrderResolver) ResolveInsertOrder() ([]string, error) {
    // Kahn's algorithm implementation
    // 親 → 子の順序でソート
}

func (r *SyncOrderResolver) ResolveDeleteOrder() ([]string, error) {
    // 子 → 親の順序でソート（逆トポロジカルソート）
}
```

**実装タスク**:
- [ ] Kahnのアルゴリズムによるトポロジカルソート実装
- [ ] INSERT用順序解決（親→子）の実装
- [ ] DELETE用順序解決（子→親）の実装
- [ ] UPDATE用順序解決（依存関係考慮）の実装
- [ ] 順序解決エラーハンドリング

### 2.2 多テーブル同期エンジン
**目標**: 複数テーブルを協調して同期する中核エンジン

```go
type MultiTableSyncEngine struct {
    db          *sql.DB
    config      MultiTableConfig
    resolver    *SyncOrderResolver
    validator   *ReferentialIntegrityValidator
}

func (e *MultiTableSyncEngine) SyncAllTables(ctx context.Context) error {
    // 1. 依存関係解決
    // 2. 順序決定
    // 3. 各テーブルの順次同期
    // 4. 参照整合性検証
}
```

**実装タスク**:
- [ ] `MultiTableSyncEngine`の実装
- [ ] テーブル間トランザクション管理
- [ ] 並列処理可能部分の特定・実装
- [ ] プログレス追跡・ログ機能
- [ ] 部分失敗時のロールバック戦略

### 2.3 外部キー解決機能
**目標**: ファイル間での外部キー値の適切な解決

```go
type ForeignKeyResolver struct {
    parentData map[string]map[string]any // table -> primaryKey -> record
    keyMappings map[string]KeyMapping
}

type KeyMapping struct {
    ParentTable   string
    ParentKey     string
    ChildTable    string
    ChildKey      string
}
```

**実装タスク**:
- [ ] 外部キー解決エンジンの実装
- [ ] 親テーブルデータのインデックス構築
- [ ] 子テーブルでの外部キー値検証
- [ ] 存在しない外部キーのエラーハンドリング
- [ ] パフォーマンス最適化（ハッシュテーブル使用）

## 📊 Phase 3: データ処理機能拡張 (2週間)

### 3.1 複数ファイル対応
**目標**: 複数のデータファイルを効率的に処理

```go
type MultiFileLoader struct {
    loaders map[string]DataLoader
    configs map[string]TableConfig
}

func (l *MultiFileLoader) LoadAllTables() (map[string][]DataRecord, error) {
    // 各テーブルのデータを並列読み込み
}
```

**実装タスク**:
- [ ] 複数ファイル同時読み込み機能
- [ ] ファイル形式混在対応（CSV・JSON混在）
- [ ] メモリ効率的な大量ファイル処理
- [ ] ファイル読み込みエラーハンドリング
- [ ] プログレスバー・読み込み状況表示

### 3.2 参照整合性チェック
**目標**: データ同期前後での参照整合性の保証

```go
type ReferentialIntegrityValidator struct {
    db    *sql.DB
    graph *DependencyGraph
}

func (v *ReferentialIntegrityValidator) ValidateBeforeSync(data map[string][]DataRecord) error {
    // ファイルデータ内での参照整合性チェック
}

func (v *ReferentialIntegrityValidator) ValidateAfterSync(ctx context.Context) error {
    // DB内での参照整合性チェック
}
```

**実装タスク**:
- [ ] ファイルデータでの参照整合性事前チェック
- [ ] DB同期後の参照整合性検証
- [ ] 整合性エラーの詳細レポート
- [ ] 自動修復機能の検討・実装
- [ ] パフォーマンス最適化

## ⚡ Phase 4: エラーハンドリング・最適化 (1-2週間)

### 4.1 高度なエラーハンドリング
**目標**: 複雑な同期処理での堅牢なエラー処理

```go
type SyncError struct {
    TableName    string
    Operation    string
    RecordID     string
    ErrorType    ErrorType
    Details      string
    Recoverable  bool
}

type ErrorType int
const (
    ReferentialIntegrityError ErrorType = iota
    DuplicateKeyError
    DataTypeError
    NetworkError
)
```

**実装タスク**:
- [ ] 構造化エラー型の実装
- [ ] エラー分類・重要度判定
- [ ] 自動リトライ機能
- [ ] エラーレポート生成
- [ ] 部分復旧戦略の実装

### 4.2 パフォーマンス最適化
**目標**: 大量データでの高速処理

**実装タスク**:
- [ ] バッチ処理サイズの最適化
- [ ] 並列処理可能部分の特定・実装
- [ ] メモリ使用量の最適化
- [ ] データベース接続プーリング
- [ ] パフォーマンス測定・プロファイリング

## 🔄 Phase 5: 統合・検証 (1-2週間)

### 5.1 既存機能との統合
**目標**: 単一テーブル機能との共存・統合

**実装タスク**:
- [ ] 設定ファイル自動判定（単一 vs 複数テーブル）
- [ ] CLI インターフェースの拡張
- [ ] 既存API の後方互換性維持
- [ ] 移行ガイドの作成
- [ ] デフォルト動作の定義

### 5.2 包括的テスト実装
**目標**: 複雑な機能の信頼性確保

```go
// E2Eテストシナリオ例
func TestParentChildSync(t *testing.T) {
    // 1. 親テーブルのみ同期
    // 2. 子テーブル同期（参照整合性チェック）
    // 3. 親削除（子存在時のエラー確認）
    // 4. 子削除→親削除の正常処理確認
}
```

**実装タスク**:
- [ ] 単体テスト（各コンポーネント）
- [ ] 統合テスト（テーブル間連携）
- [ ] E2Eテスト（実際のユースケース）
- [ ] パフォーマンステスト
- [ ] エラーシナリオテスト

### 5.3 ドキュメント・使用例
**目標**: 使いやすい機能として提供

**実装タスク**:
- [ ] 設定ファイルリファレンス
- [ ] 使用例・チュートリアル
- [ ] トラブルシューティングガイド
- [ ] API ドキュメント
- [ ] ベストプラクティス集

## 📅 実装スケジュール (7-10週間)

| Phase | 期間 | 主要成果物 |
|-------|------|------------|
| Phase 1 | 週1-2 | 多テーブル設定・依存関係グラフ |
| Phase 2 | 週3-5 | 同期エンジン・順序解決 |
| Phase 3 | 週6-7 | データ処理・整合性チェック |
| Phase 4 | 週8-9 | エラーハンドリング・最適化 |
| Phase 5 | 週10 | 統合・テスト・ドキュメント |

## 🎯 成功指標

- [ ] **機能性**: 3層以上の親子関係テーブルでの正常同期
- [ ] **パフォーマンス**: 100万レコード規模での10分以内処理
- [ ] **信頼性**: 参照整合性エラー0%達成
- [ ] **使いやすさ**: 既存ユーザーの移行コスト最小化
- [ ] **保守性**: 90%以上のテストカバレッジ達成

この計画により、現在の単一テーブル同期機能を、参照整合性を保証する包括的な多テーブル同期システムへと発展させることができます。