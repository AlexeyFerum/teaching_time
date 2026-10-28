#!/usr/bin/env bash
# sql-anon.sh — обезличивание SQL-скриптов перед отправкой в ИИ-агенты.
#
# Что делает:
#   * заменяет имена баз, схем, таблиц, колонок, алиасов и пользовательских
#     функций на нейтральные: schema_001, table_001, column_001, ...
#   * заменяет строковые литералы на 'val_001' (в них часто лежат реальные данные)
#   * удаляет комментарии (-- и /* */)
#   * сохраняет таблицу соответствий, чтобы вернуть имена обратно (-r)
#
# Использование:
#   ./sql-anon.sh [-m map] [-k keep] [-N] [-b] [файл.sql] > anon.sql
#   ./sql-anon.sh -r [-m map] [ответ_ии.txt]           > restored.txt
#
#   -m FILE  файл соответствий (по умолчанию ./sql-anon.map). Если есть —
#            загружается, так что имена остаются стабильными между запусками.
#   -k FILE  файл с именами, которые НЕ нужно заменять (по одному на строку,
#            # — комментарий), например: id, created_at, updated_at
#   -N       маскировать числовые литералы из 4+ цифр (ID, суммы)
#   -b       поддержка идентификаторов в [квадратных скобках] (MS SQL)
#   -r       обратный режим: подставить оригинальные имена в текст
#   -h       справка
#
# ВАЖНО: файл соответствий содержит оригиналы — НИКОГДА не отправляйте его в ИИ.
#
# Требования: bash 4+, UTF-8 локаль (для кириллицы в именах без кавычек).
# Внешние утилиты: только locale и sort (есть в любой системе с bash).

umask 077

if (( BASH_VERSINFO[0] < 4 )); then
  echo "Нужен bash 4+ (на macOS: brew install bash)" >&2
  exit 1
fi

# UTF-8 локаль, чтобы [[:alpha:]] понимал кириллицу
for _l in C.UTF-8 C.utf8 en_US.UTF-8 en_US.utf8 ru_RU.UTF-8 ru_RU.utf8; do
  if [[ $(LC_ALL=$_l locale charmap 2>/dev/null) == UTF-8 ]]; then
    export LC_ALL=$_l
    break
  fi
done

usage() { sed -n '2,/^umask/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

MAP=./sql-anon.map KEEPFILE= MASKNUM=0 REVERSE=0 BRACKETS=0
while getopts ':m:k:Nrbh' o; do
  case $o in
    m) MAP=$OPTARG ;;
    k) KEEPFILE=$OPTARG ;;
    N) MASKNUM=1 ;;
    r) REVERSE=1 ;;
    b) BRACKETS=1 ;;
    h) usage; exit 0 ;;
    *) usage >&2; exit 1 ;;
  esac
done
shift $((OPTIND - 1))
INPUT=${1:--}

if [[ $INPUT == - ]]; then
  mapfile -t LINES
else
  [[ -r $INPUT ]] || { echo "Не могу прочитать: $INPUT" >&2; exit 1; }
  mapfile -t LINES < "$INPUT"
fi

declare -A KW FN KEEP PH REV PK PN CNT
declare -A PRI=([database]=6 [schema]=5 [table]=4 [func]=3 [column]=2 [alias]=1)
declare -a ORDER=() FL=()

# ---------------------------------------------------------------- словари
for w in SELECT FROM WHERE AND OR NOT IN IS NULL LIKE ILIKE BETWEEN EXISTS CASE WHEN THEN ELSE END \
  AS ON JOIN INNER LEFT RIGHT FULL OUTER CROSS NATURAL USING GROUP BY ORDER HAVING LIMIT OFFSET \
  FETCH FIRST NEXT ROWS ROW ONLY UNION ALL INTERSECT EXCEPT MINUS DISTINCT TOP INSERT INTO VALUES \
  UPDATE SET DELETE MERGE MATCHED CREATE ALTER DROP TRUNCATE TABLE VIEW INDEX UNIQUE PRIMARY KEY \
  FOREIGN REFERENCES CONSTRAINT DEFAULT CHECK ADD COLUMN RENAME TO IF TEMP TEMPORARY MATERIALIZED \
  UNLOGGED SCHEMA DATABASE WITH RECURSIVE OVER PARTITION WINDOW RANGE UNBOUNDED PRECEDING FOLLOWING \
  CURRENT ASC DESC NULLS LAST BEGIN COMMIT ROLLBACK TRANSACTION GRANT REVOKE RETURNING LATERAL ANY \
  SOME TRUE FALSE CAST FILTER WITHIN GROUPS QUALIFY CONCURRENTLY REPLACE CASCADE RESTRICT \
  VARCHAR CHAR TEXT INT INTEGER BIGINT SMALLINT NUMERIC DECIMAL FLOAT REAL DOUBLE PRECISION BOOLEAN \
  BOOL DATE TIME TIMESTAMP TIMESTAMPTZ INTERVAL SERIAL BIGSERIAL UUID JSON JSONB BYTEA BLOB CLOB \
  NVARCHAR NUMBER VARCHAR2 TINYINT DATETIME ZONE YEAR MONTH DAY HOUR MINUTE SECOND \
  CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP LOCALTIME LOCALTIMESTAMP \
  FOR LOOP DECLARE RETURN RETURNS FUNCTION PROCEDURE TRIGGER LANGUAGE WHILE EXECUTE CALL DO \
  BOTH LEADING TRAILING ESCAPE COLLATE ISNULL NOTNULL OVERLAPS; do
  KW[$w]=1
done

for w in COUNT SUM AVG MIN MAX COALESCE NULLIF GREATEST LEAST CONCAT CONCAT_WS SUBSTRING SUBSTR \
  LENGTH LEN LOWER UPPER TRIM LTRIM RTRIM ROUND FLOOR CEIL CEILING ABS MOD POWER SQRT EXP LN LOG \
  SIGN NOW DATE_TRUNC DATE_PART EXTRACT TO_CHAR TO_DATE TO_NUMBER TO_TIMESTAMP DATEADD DATEDIFF \
  DATE_ADD DATE_SUB DATE_FORMAT ADD_MONTHS MONTHS_BETWEEN GETDATE SYSDATE ROW_NUMBER RANK DENSE_RANK \
  NTILE LAG LEAD FIRST_VALUE LAST_VALUE NTH_VALUE PERCENT_RANK CUME_DIST STRING_AGG ARRAY_AGG \
  LISTAGG GROUP_CONCAT JSON_AGG JSONB_AGG JSON_OBJECT JSON_ARRAY IFNULL NVL NVL2 DECODE POSITION \
  INSTR CHARINDEX SPLIT_PART REGEXP_REPLACE REGEXP_LIKE REGEXP_SUBSTR UNNEST GENERATE_SERIES \
  LPAD RPAD FORMAT TRUNC IIF OVERLAY AGE STDDEV VARIANCE MEDIAN PERCENTILE_CONT PERCENTILE_DISC \
  BOOL_AND BOOL_OR EVERY TO_JSON TO_JSONB JSON_EXTRACT JSON_VALUE ARRAY RANDOM RAND MD5 TRY_CAST \
  CONVERT STUFF REVERSE REPEAT ASCII CHR INITCAP TRANSLATE STRPOS; do
  FN[$w]=1
done

if [[ -n $KEEPFILE ]]; then
  [[ -r $KEEPFILE ]] || { echo "Не могу прочитать: $KEEPFILE" >&2; exit 1; }
  while IFS= read -r l; do
    l=${l%%#*}
    l=${l//[[:space:]]/}
    [[ -n $l ]] && KEEP["${l,,}"]=1
  done < "$KEEPFILE"
fi

# ---------------------------------------------------------------- регулярки
P_ID='[[:alpha:]_][[:alnum:]_$#]*'
P_DQ='"([^"]|"")*"'
P_BT='`[^`]*`'
P_BR='\[[[:alpha:]_][^]]*\]'
if (( BRACKETS )); then
  P_ANY="($P_ID|$P_DQ|$P_BT|$P_BR)"
else
  P_ANY="($P_ID|$P_DQ|$P_BT)"
fi
RE_PART="^$P_ANY"
RE_CHAIN="^$P_ANY(\\.($P_ANY|\\*))*"
RE_STR="^'(([^']|'')*)'"
RE_STR_TAIL="^(([^']|'')*)'"
RE_NUM='^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?'
RE_CTE='^[[:space:]]+[Aa][Ss][[:space:]]*\('

# ---------------------------------------------------------------- утилиты
esc()   { local s=$1; s=${s//\\/\\\\}; s=${s//$'\n'/\\n}; s=${s//$'\t'/\\t}; REPLY=$s; }
unesc() { printf -v REPLY '%b' "$1"; }

# снять кавычки с идентификатора -> NAME
norm() {
  local s=$1
  case ${s:0:1} in
    '"') s=${s:1:${#s}-2}; s=${s//\"\"/\"} ;;
    '`') s=${s:1:${#s}-2} ;;
    '[') s=${s:1:${#s}-2} ;;
  esac
  NAME=$s
}

load_map() {
  [[ -f $MAP ]] || return 0
  local ph orig key n kind
  while IFS=$'\t' read -r ph orig; do
    [[ -z $ph ]] && continue
    unesc "$orig"; orig=$REPLY
    if [[ $ph =~ ^[0-9]+$ ]]; then
      key="n:$orig"; n=$((ph - 900000))
      (( n > ${CNT[num]:-0} )) && CNT[num]=$n
    elif [[ $ph =~ ^val_([0-9]+)$ ]]; then
      key="s:$orig"; n=$((10#${BASH_REMATCH[1]}))
      (( n > ${CNT[val]:-0} )) && CNT[val]=$n
    elif [[ $ph =~ ^([a-z]+)_([0-9]+)$ ]]; then
      kind=${BASH_REMATCH[1]}; n=$((10#${BASH_REMATCH[2]}))
      (( n > ${CNT[$kind]:-0} )) && CNT[$kind]=$n
      norm "$orig"; key="i:${NAME,,}"
    else
      continue
    fi
    PH["$key"]=$ph
    REV["$ph"]=$orig
  done < "$MAP"
}

write_map() {
  local ph
  {
    for ph in "${!REV[@]}"; do
      esc "${REV[$ph]}"
      printf '%s\t%s\n' "$ph" "$REPLY"
    done
  } | LC_ALL=C sort > "$MAP"
  chmod 600 "$MAP" 2>/dev/null
}

# ---------------------------------------------------------------- обратный режим
if (( REVERSE )); then
  [[ -f $MAP ]] || { echo "Нет файла соответствий: $MAP" >&2; exit 1; }
  load_map
  for line in "${LINES[@]}"; do
    rest=$line OUT=
    while [[ $rest =~ ^([^[:alnum:]_]*)([[:alnum:]_]+) ]]; do
      pre=${BASH_REMATCH[1]} w=${BASH_REMATCH[2]}
      rest=${rest:$(( ${#pre} + ${#w} ))}
      OUT+=$pre
      if [[ -n ${REV["$w"]+x} ]]; then OUT+=${REV["$w"]}; else OUT+=$w; fi
    done
    printf '%s\n' "$OUT$rest"
  done
  exit 0
fi

# ---------------------------------------------------------------- регистрация имён
# Проход 1: собираем имена и определяем «самый сильный» тип каждого
# (схема > таблица > функция > колонка > алиас), чтобы нумерация была осмысленной.
reg() {
  local key=$1 kind=$2 raw=$3
  [[ -n ${PH["$key"]+x} ]] && return
  if [[ -z ${PK["$key"]+x} ]]; then
    PK["$key"]=$kind; PN["$key"]=$raw; ORDER+=("$key")
  elif (( PRI[$kind] > PRI[${PK["$key"]}] )); then
    PK["$key"]=$kind
  fi
}

finalize() {
  local key kind n ph
  for key in "${ORDER[@]}"; do
    kind=${PK["$key"]}
    n=$(( ${CNT[$kind]:-0} + 1 )); CNT[$kind]=$n
    printf -v ph '%s_%03d' "$kind" "$n"
    PH["$key"]=$ph
    REV["$ph"]=${PN["$key"]}
  done
}

put_name() {   # $1 тип, $2 исходная запись (с кавычками, если были)
  local kind=$1 raw=$2 lc key
  norm "$raw"; lc=${NAME,,}
  if [[ -n ${KEEP["$lc"]+x} ]]; then OUT+=$raw; return; fi
  key="i:$lc"
  if (( PASS == 1 )); then
    reg "$key" "$kind" "$raw"
    OUT+=$raw
  else
    OUT+=${PH["$key"]}
  fi
}

put_lit() {    # $1 s|n, $2 значение -> REPLY
  local key="$1:$2" n ph
  if [[ -z ${PH["$key"]+x} ]]; then
    if [[ $1 == s ]]; then
      n=$(( ${CNT[val]:-0} + 1 )); CNT[val]=$n; printf -v ph 'val_%03d' "$n"
    else
      n=$(( ${CNT[num]:-0} + 1 )); CNT[num]=$n; ph=$(( 900000 + n ))
    fi
    PH["$key"]=$ph; REV["$ph"]=$2
  fi
  REPLY=${PH["$key"]}
}

lit_out() {
  if [[ -z $1 ]]; then OUT+="''"; else put_lit s "$1"; OUT+="'$REPLY'"; fi
  PREV= AFTER=0 EXPECT=0
}

# ---------------------------------------------------------------- контекст SQL
# EXPECT=1 — следующее имя это таблица; AFTER=1 — следующее имя это алиас;
# FL[глубина]=1 — мы внутри списка FROM/INTO (запятая => снова таблица)
kw_effects() {
  local k=$1
  case $k in
    FROM)
      if [[ $PREV == DISTINCT ]]; then EXPECT=0; else EXPECT=1; FL[DEPTH]=1; fi
      AFTER=0 ;;
    INTO|UPDATE|TABLE|TRUNCATE) EXPECT=1 AFTER=0; FL[DEPTH]=1 ;;
    JOIN|VIEW|REFERENCES)       EXPECT=1 AFTER=0 ;;
    ON)  if (( IDX )); then EXPECT=1; else EXPECT=0; fi; FL[DEPTH]=0; AFTER=0 ;;
    INDEX) IDX=1 EXPECT=0 AFTER=0 ;;
    AS)  EXPECT=0 AFTER=1 ;;
    IF|EXISTS|NOT|ONLY|TEMP|TEMPORARY|MATERIALIZED|UNLOGGED|LATERAL|OR|REPLACE|RECURSIVE|CONCURRENTLY|UNIQUE|INNER|LEFT|RIGHT|FULL|OUTER|CROSS|NATURAL|DISTINCT) ;;
    WHERE|GROUP|ORDER|HAVING|SELECT|SET|VALUES|USING|UNION|INTERSECT|EXCEPT|MINUS|LIMIT|OFFSET|RETURNING|WINDOW|QUALIFY|WHEN|THEN|ELSE|CASE|END|AND|BY)
      FL[DEPTH]=0 EXPECT=0 AFTER=0 ;;
    *) EXPECT=0 AFTER=0 ;;
  esac
  PREV=$k
}

# Цепочка a.b.c / "a"."b" / t.*
handle_chain() {
  local chain=$1 after=$2 c=$1 p up i n k
  local star=0 isfn=0 tbl=0
  local -a parts=()

  while [[ $c =~ $RE_PART ]]; do
    p=${BASH_REMATCH[0]}; parts+=("$p"); c=${c:${#p}}
    [[ $c == .* ]] || break
    c=${c:1}
    if [[ $c == '*' ]]; then star=1; break; fi
  done
  n=${#parts[@]}

  [[ $after =~ ^[[:space:]]*\( ]] && isfn=1

  if (( n == 1 && star == 0 )); then
    p=${parts[0]}
    case ${p:0:1} in
      '"'|'`'|'[') ;;                       # в кавычках — всегда имя
      *)
        up=${p^^}
        if [[ -n ${KW["$up"]+x} ]]; then OUT+=$p; kw_effects "$up"; return; fi
        if (( isfn )) && [[ -n ${FN["$up"]+x} ]]; then
          OUT+=$p EXPECT=0 AFTER=0 PREV=; return
        fi ;;
    esac
  fi

  (( EXPECT )) && tbl=1

  for (( i = 0; i < n; i++ )); do
    if (( i == n - 1 )); then
      if   (( star && ! tbl )); then k=alias
      elif (( tbl ));  then k=table
      elif (( isfn )); then k=func
      elif (( n == 1 )); then
        if   (( AFTER )); then k=alias
        elif [[ $after =~ $RE_CTE ]]; then k=table
        else k=column; fi
      else k=column; fi
    elif (( i == n - 2 )); then
      if   (( tbl || isfn )); then k=schema
      elif (( n == 2 )); then k=alias
      else k=table; fi
    elif (( i == n - 3 )); then
      if (( tbl || isfn )); then k=database; else k=schema; fi
    else
      k=database
    fi
    put_name "$k" "${parts[i]}"
    (( i < n - 1 )) && OUT+=.
  done
  (( star )) && OUT+='.*'

  if (( tbl )); then EXPECT=0 AFTER=1; else EXPECT=0 AFTER=0; fi
  PREV=
}

# ---------------------------------------------------------------- сканер
scan_line() {
  local rest=$1 m v c
  OUT=
  if (( IN_STR )) && [[ -z $rest ]]; then STRBUF+=$'\n'; fi

  while [[ -n $rest ]]; do
    # внутри /* ... */
    if (( IN_BLOCK )); then
      if [[ $rest == *'*/'* ]]; then rest=${rest#*'*/'}; IN_BLOCK=0; else rest=; fi
      continue
    fi
    # внутри многострочной строки
    if (( IN_STR )); then
      if [[ $rest =~ $RE_STR_TAIL ]]; then
        m=${BASH_REMATCH[0]}; STRBUF+=${BASH_REMATCH[1]}
        rest=${rest:${#m}}; IN_STR=0
        lit_out "$STRBUF"
      else
        STRBUF+=$rest$'\n'; rest=
      fi
      continue
    fi
    # пробелы
    if [[ $rest =~ ^[[:space:]]+ ]]; then
      m=${BASH_REMATCH[0]}; OUT+=$m; rest=${rest:${#m}}; continue
    fi
    # комментарии
    case $rest in
      '--'*) rest=; continue ;;
      '/*'*) IN_BLOCK=1; rest=${rest:2}; continue ;;
    esac
    # строковый литерал
    if [[ $rest == \'* ]]; then
      if [[ $rest =~ $RE_STR ]]; then
        m=${BASH_REMATCH[0]}; v=${BASH_REMATCH[1]}
        rest=${rest:${#m}}
        lit_out "$v"
      else
        IN_STR=1; STRBUF=${rest:1}$'\n'; rest=
      fi
      continue
    fi
    # число
    if [[ $rest =~ $RE_NUM ]]; then
      m=${BASH_REMATCH[0]}; rest=${rest:${#m}}
      if (( MASKNUM )) && [[ $m =~ ^[0-9]{4,}$ ]]; then
        put_lit n "$m"; OUT+=$REPLY
      else
        OUT+=$m
      fi
      PREV= AFTER=0 EXPECT=0
      continue
    fi
    # идентификатор / цепочка a.b.c
    if [[ $rest =~ $RE_CHAIN ]]; then
      m=${BASH_REMATCH[0]}; rest=${rest:${#m}}
      handle_chain "$m" "$rest"
      continue
    fi
    # прочие символы
    c=${rest:0:1}; rest=${rest:1}; OUT+=$c
    case $c in
      '(') DEPTH=$((DEPTH + 1)); FL[DEPTH]=0; EXPECT=0 AFTER=0 ;;
      ')') FL[DEPTH]=0; (( DEPTH > 0 )) && DEPTH=$((DEPTH - 1)); EXPECT=0 AFTER=${FL[DEPTH]:-0} ;;
      ',') if [[ ${FL[DEPTH]:-0} == 1 ]]; then EXPECT=1; else EXPECT=0; fi; AFTER=0 ;;
      ';') DEPTH=0; FL=(); EXPECT=0 AFTER=0 IDX=0 ;;
      *)   EXPECT=0 AFTER=0 ;;
    esac
    PREV=
  done
}

run_pass() {
  PASS=$1 IN_BLOCK=0 IN_STR=0 STRBUF= DEPTH=0 EXPECT=0 AFTER=0 IDX=0 PREV=
  FL=()
  local line
  for line in "${LINES[@]}"; do
    scan_line "$line"
    (( PASS == 2 )) && printf '%s\n' "$OUT"
  done
}

# ---------------------------------------------------------------- main
load_map
run_pass 1
finalize
run_pass 2
write_map

{
  printf 'Готово. Таблицы: %d, колонки: %d, схемы: %d, функции: %d, алиасы: %d, строки: %d\n' \
    "${CNT[table]:-0}" "${CNT[column]:-0}" "${CNT[schema]:-0}" "${CNT[func]:-0}" \
    "${CNT[alias]:-0}" "${CNT[val]:-0}"
  printf 'Карта соответствий: %s (содержит оригиналы — в ИИ не отправлять!)\n' "$MAP"
} >&2