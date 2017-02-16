module Data.MySQL.Language where

import Data.Foldable (intercalate)
import Data.List.NonEmpty (NonEmptyList)
import Data.Maybe (Maybe(..))
import Prelude (map, pure, (<>), ($))


-- https://dev.mysql.com/doc/refman/5.7/en/select.html
data SqlSelect =
  SqlSelect
    SelectExpr
    From
    (Maybe Where)
    (Maybe GroupBy)
    (Maybe Having)
    (Maybe OrderBy)

newtype SelectExpr = SelectExpr (NonEmptyList Column)

type Column = String

-- https://dev.mysql.com/doc/refman/5.7/en/join.html
data From = From TableReferences

toSqlStringFrom :: From -> String
toSqlStringFrom (From tables)
  = "FROM " <> (intercalate "," (map toSqlTableReference tables))

type TableReferences = NonEmptyList TableReference

data TableReference
  = Table TableFactor
  | Join JoinTable

-- !!! Move to this one.
--data TableReference2
--  = Table2 Table
--  | Derived Subquery
--  | Join 

toSqlTableReference :: TableReference -> String
toSqlTableReference (Table t)
  = toSqlTableFactor t
toSqlTableReference (Join j)
  = toSqlJoinTable j

--innerJoin :: TableReference -> TableReference -> TableReference
--innerJoin tr1@(Table tf1) tr2@(Table tf2) = Join (InnerJoin tr1 )

data JoinTable
  = InnerJoin TableReference TableFactor JoinCondition
  | CrossJoin TableReference TableFactor JoinCondition
  | StraightJoin TableReference TableFactor -- On ConditionalExpr
  | LeftJoin TableReference JoinCondition
  | LeftOuterJoin TableReference JoinCondition
  | RightJoin TableReference JoinCondition
  | RightOuterJoin TableReference JoinCondition
  | NaturalLeftJoin TableReference
  | NaturalLeftOuterJoin TableReference
  | NaturalRightJoin TableReference
  | NaturalRightOuterJoin TableReference

toSqlJoinTable :: JoinTable -> String
toSqlJoinTable (InnerJoin tr tf jc)
  = toSqlTableReference tr <> " INNER JOIN " <> toSqlTableFactor tf <> " " <> toSqlJoinCondition jc
toSqlJoinTable _
  = "Implement in toSqlJoinTable"

type ColumnList = NonEmptyList String -- Columns must exist in both tables.

toSqlColumnList :: NonEmptyList String -> String
toSqlColumnList cols = intercalate "," cols

data JoinCondition
  = On ConditionalExpr
  | Using ColumnList

toSqlJoinCondition :: JoinCondition -> String
toSqlJoinCondition (On c)
  = "ON " <> toSqlConditionalExpr c
toSqlJoinCondition (Using c)
  = "USING " <> toSqlColumnList c

-- Any conditional expression of the form that can be used in a WHERE clause.
newtype ConditionalExpr = ConditionalExpr String

toSqlConditionalExpr :: ConditionalExpr -> String
toSqlConditionalExpr (ConditionalExpr c)
  = c

newtype Alias = Alias String

data TableFactor
  = Name String Alias
  | Subquery String Alias
  | Tables TableReferences

toSqlTableFactor :: TableFactor -> String
toSqlTableFactor (Name n (Alias a))
  = n <> " " <> a
toSqlTableFactor (Subquery n (Alias a))
  = n <> " " <> a -- filler
toSqlTableFactor (Tables trs)
  = intercalate "," (map toSqlTableReference trs)

data SqlTable
  = SqlAsset
  | SqlPricing
  | SqlUser
  | SqlKeyAttribute

-- http://savage.net.au/SQL/sql-92.bnf.html#left paren
newtype Where
  = Where SearchCondition

data SearchCondition
  = One BooleanTerm
  | Or SearchCondition BooleanTerm

toSqlSearchCondition :: SearchCondition -> String
toSqlSearchCondition (One b)
  = toSqlBooleanTerm b
toSqlSearchCondition (Or c b)
  = toSqlSearchCondition c <> " OR " <> toSqlBooleanTerm b

data BooleanTerm
  = Check BooleanFactor
  | And BooleanTerm BooleanFactor

toSqlBooleanTerm :: BooleanTerm -> String
toSqlBooleanTerm (Check b)
  = toSqlBooleanFactor b
toSqlBooleanTerm (And t b)
  = toSqlBooleanTerm t <> " AND " <> toSqlBooleanFactor b

data BooleanFactor
--  = Is BooleanTest
--  | Not BooleanTest
  = Is BooleanPrimary
  | Not BooleanPrimary

toSqlBooleanFactor :: BooleanFactor -> String
toSqlBooleanFactor (Is b)
--  = toSqlBooleanTest b
  = toSqlBooleanPrimary b
toSqlBooleanFactor (Not b)
--  = "NOT " <> toSqlBooleanTest b
  = "NOT " <> toSqlBooleanPrimary b

--data BooleanTest
--  = Is BooleanPrimary Boolean
--  | IsNot BooleanPrimary Boolean

--toSqlBooleanTest :: BooleanTest -> String
--toSqlBooleanTest (Is bp b)
--  = toSqlBooleanPrimary bp <> " IS " <> toSqlBoolean b
--toSqlBooleanTest (IsNot bp b)
--  = toSqlBooleanPrimary bp <> " IS NOT " <> toSqlBoolean b

toSqlBoolean :: Boolean -> String
toSqlBoolean true = "TRUE"
toSqlBoolean false = "FALSE"

data BooleanPrimary
  = Predicate Predicate
  | Condition SearchCondition

toSqlBooleanPrimary :: BooleanPrimary -> String
toSqlBooleanPrimary (Predicate p)
  = toSqlPredicate p
toSqlBooleanPrimary (Condition c)
  = toSqlSearchCondition c


data Predicate
  = Ord OrdComparison ColumnOrValue ColumnOrValue
  -- Can add these as needed.
  -- | Between PredicateBetween
  -- | In PredicateIn
  -- | Like PredicateLike
  -- | Null PredicateNull
  -- | QComparison PredicateQuantifiedComparison
  -- | Exists PredicateExists
  -- | Match PredicateMatch
  -- | Overlaps PredicateOverlaps

toSqlPredicate :: Predicate -> String
toSqlPredicate (Ord c cv1 cv2)
  = toSqlColumnOrValue cv1 <> " " <> toSqlComparison c <> " " <> toSqlColumnOrValue cv2

--data PredicateOrd
--  = PredicateOrd OrdComparison ColumnOrValue ColumnOrValue

--  | And (Maybe WhereCondition) (Maybe WhereCondition)
--  | Or (Maybe WhereCondition) (Maybe WhereCondition)

toSqlWhere :: Where -> String
toSqlWhere (Where s)
  = "WHERE " <> toSqlSearchCondition s

data WhereCondition
  = W OrdComparison ColumnOrValue ColumnOrValue

toSqlWhereCondition :: WhereCondition -> String
toSqlWhereCondition (W c cv1 cv2)
  = toSqlColumnOrValue cv1 <> " " <> toSqlComparison c <> " " <> toSqlColumnOrValue cv2

-- https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html
data OrdComparison
  = EQ
  | NE
  | GT
  | GTE
  | LT
  | LTE

toSqlComparison :: OrdComparison -> String
toSqlComparison c
  = case c of
    EQ -> "="
    NE -> "<>"
    GT -> ">"
    GTE -> ">="
    LT -> "<"
    LTE -> "<="

data ColumnOrValue
  = Column String
  | Value String

toSqlColumnOrValue :: ColumnOrValue -> String
toSqlColumnOrValue (Column c) = c
toSqlColumnOrValue (Value v) = v

newtype GroupBy = GroupBy (NonEmptyList GroupByColumn)

toSqlGroupBy :: GroupBy -> String
toSqlGroupBy (GroupBy cols)
  = "GROUP BY " <> (intercalate "," (map toSqlGroupByColumn cols))

data GroupByColumn = GroupByColumn Column AscDesc

toSqlGroupByColumn :: GroupByColumn -> String
toSqlGroupByColumn (GroupByColumn c o)
  = c <> " " <> toSqlAscDesc o

data AscDesc
  = Asc
  | Desc

toSqlAscDesc :: AscDesc -> String
toSqlAscDesc Asc = "ASC"
toSqlAscDesc Desc = "DESC"

newtype Having = Having WhereCondition

newtype OrderBy = OrderBy (NonEmptyList OrderByColumn)
data OrderByColumn = OrderByColumn Column AscDesc


toSqlStringSelect :: SelectExpr -> String
toSqlStringSelect (SelectExpr cols)
  = "SELECT " <> (intercalate "," cols)

toSqlString :: SqlSelect -> String
toSqlString
  (SqlSelect
    sel
    from
    (Just whereCond)
    (Just groupBy)
    (Just (Having whereCondHaving))
    (Just (OrderBy orderBys))
  )
  = toSqlStringSelect sel
  <> " " <> toSqlStringFrom from
  <> " " <> toSqlWhere whereCond
  <> " " <> toSqlGroupBy groupBy
toSqlString _ = ""

innerJoin :: TableReference -> TableFactor -> JoinCondition -> TableReference
innerJoin tr tf jc =
  Join (
    InnerJoin tr tf jc
  )

testSql :: SqlSelect
testSql =
  SqlSelect selectCols from where' groupBy having orderBy
    where
      selectCols = SelectExpr (pure "id")
      --from = From $ pure $ Table $ Name "asset" (Alias "a")
      from = From $ pure $ Join (InnerJoin (Table (Name "asset" (Alias "a"))) (Name "pricing" (Alias "p")) (On (ConditionalExpr "a = b")))
      --from = From $ pure $ (Table (Name "asset" (Alias "a"))) `innerJoin` (Name "pricing" (Alias "p")) (On (ConditionalExpr "a = b"))
      --where' = Just $ And Nothing $ Just (W EQ (Column "id") (Value "123"))
      where' = Just $ Where $ One (Check (Is (Predicate (Ord EQ (Column "id") (Value "123") )) ))
      groupBy = Just $ GroupBy $ pure $ GroupByColumn "id" Asc
      having = Just $ Having $ W EQ (Column "id") (Value "123")
      orderBy = Just $ OrderBy $ pure $ OrderByColumn "id" Asc
