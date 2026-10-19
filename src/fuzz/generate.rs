//! Deterministic generation of one fuzz case: a query plus the planner and
//! executor settings it runs under. Everything is derived from the case seed.

use fastrand::Rng;

/// Tables created by [`SCHEMA`] with the columns usable in predicates and joins.
const TABLES: [(&str, [&str; 3]); 4] = [
    ("a", ["id", "g", "k"]),
    ("b", ["id", "a_id", "g"]),
    ("c", ["id", "b_id", "v"]),
    ("p", ["id", "g", "g"]),
];

/// Deterministic test data (no `random()`), so the same case always plans and
/// executes the same way. `ANALYZE` makes the plan choice stable.
pub const SCHEMA: &str = "
CREATE TABLE a AS SELECT i AS id, i % 7 AS g, i % 100 AS k, md5(i::text) AS s FROM generate_series(1, 3000) i;
CREATE TABLE b AS SELECT i AS id, (i * 7) % 3000 AS a_id, i % 13 AS g FROM generate_series(1, 5000) i;
CREATE TABLE c AS SELECT i AS id, i % 50 AS b_id, (i * 37) % 100 AS v FROM generate_series(1, 800) i;
CREATE INDEX ON a (id); CREATE INDEX ON a (k); CREATE INDEX ON a (g);
CREATE INDEX ON b (a_id); CREATE INDEX ON b (g);
CREATE INDEX ON c (b_id);
CREATE TABLE p (id int, g int) PARTITION BY RANGE (id);
CREATE TABLE p1 PARTITION OF p FOR VALUES FROM (0) TO (1000);
CREATE TABLE p2 PARTITION OF p FOR VALUES FROM (1000) TO (2000);
CREATE TABLE p3 PARTITION OF p FOR VALUES FROM (2000) TO (3000);
INSERT INTO p SELECT i, i % 5 FROM generate_series(0, 2999) i;
CREATE INDEX ON p (id);
ANALYZE a; ANALYZE b; ANALYZE c; ANALYZE p";

const ENABLE_GUCS: [&str; 13] = [
    "enable_hashjoin",
    "enable_mergejoin",
    "enable_nestloop",
    "enable_seqscan",
    "enable_indexscan",
    "enable_bitmapscan",
    "enable_hashagg",
    "enable_sort",
    "enable_material",
    "enable_memoize",
    "enable_indexonlyscan",
    "enable_partitionwise_join",
    "enable_incremental_sort",
];

/// One generated test case.
pub struct Case {
    /// Settings that differ from the defaults, applied with `SET LOCAL`.
    pub settings: Vec<(&'static str, String)>,
    pub query: String,
}

impl Case {
    /// The case for `seed`; the same seed always gives the same case.
    pub fn generate(seed: u64) -> Self {
        let mut gen_ = Generator(Rng::with_seed(seed));
        let settings = gen_.settings();
        let query = if gen_.0.u8(..100) < 15 {
            gen_.dml()
        } else {
            gen_.select()
        };
        Self { settings, query }
    }

    /// `name=value, ...` of the non-default settings, for logs.
    pub fn settings_text(&self) -> String {
        self.settings
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

struct Generator(Rng);

impl Generator {
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.0.usize(..items.len())]
    }

    fn settings(&mut self) -> Vec<(&'static str, String)> {
        let mut settings = Vec::new();
        for _ in 0..self.0.usize(..=4) {
            let name = *self.pick(&ENABLE_GUCS);
            if !settings.iter().any(|(n, _)| *n == name) {
                settings.push((name, "off".to_owned()));
            }
        }
        if self.0.u8(..100) < 25 {
            settings.push(("work_mem", format!("{}kB", self.pick(&[64, 256, 1024]))));
        }
        match self.0.u8(..100) {
            0..30 => settings.push(("max_parallel_workers_per_gather", "0".to_owned())),
            // Make parallel plans attractive on these small tables.
            30..65 => settings.extend([
                ("parallel_setup_cost", "0".to_owned()),
                ("parallel_tuple_cost", "0".to_owned()),
                ("min_parallel_table_scan_size", "0".to_owned()),
                ("min_parallel_index_scan_size", "0".to_owned()),
            ]),
            _ => {}
        }
        settings
    }

    fn predicate(&mut self, alias: &str, table: usize) -> String {
        let column = format!("{alias}.{}", self.pick(&TABLES[table].1));
        match self.0.u8(..5) {
            0 => format!("{column} < {}", self.0.u32(0..=3000)),
            1 => format!("{column} = {}", self.0.u32(0..=20)),
            2 => format!("{column} % {} = 0", self.0.u32(2..=9)),
            3 => format!(
                "{column} IN ({}, {})",
                self.0.u32(0..=9),
                self.0.u32(10..=99)
            ),
            _ => format!(
                "{column} BETWEEN {} AND {}",
                self.0.u32(0..=50),
                self.0.u32(51..=500)
            ),
        }
    }

    fn select(&mut self) -> String {
        let n = *self.pick(&[1, 1, 2, 2, 3]);
        let tables: Vec<usize> = (0..n).map(|_| self.0.usize(..TABLES.len())).collect();
        let aliases: Vec<String> = (0..n).map(|i| format!("t{i}")).collect();

        let mut from = format!("{} {}", TABLES[tables[0]].0, aliases[0]);
        for i in 1..n {
            let join = *self.pick(&["JOIN", "LEFT JOIN", "JOIN"]);
            let left = self.pick(&TABLES[tables[i - 1]].1);
            let right = self.pick(&TABLES[tables[i]].1);
            from += &format!(
                " {join} {} {} ON {}.{left} = {}.{right}",
                TABLES[tables[i]].0,
                aliases[i],
                aliases[i - 1],
                aliases[i]
            );
        }
        let mut predicates = Vec::new();
        for i in 0..n {
            if self.0.u8(..100) < 60 {
                predicates.push(self.predicate(&aliases[i], tables[i]));
            }
        }
        let filter = predicates.join(" AND ");
        let where_ = if filter.is_empty() {
            String::new()
        } else {
            format!(" WHERE {filter}")
        };
        let and_ = if filter.is_empty() { " WHERE" } else { " AND" };
        let mut cols = Vec::new();
        for i in 0..n {
            cols.push(format!("{}.{}", aliases[i], self.pick(&TABLES[tables[i]].1)));
        }
        let first = &cols[0];
        let last = &cols[n - 1];
        let all = cols.join(", ");
        let t0 = &aliases[0];
        let m = self.0.u32(1..=12);

        match self.0.u8(..40) {
            0 | 1 => format!("SELECT {all} FROM {from}{where_}"),
            2 | 3 => {
                let having = *self.pick(&["", "HAVING count(*) > 1"]);
                format!("SELECT {first}, count(*), sum({last}) FROM {from}{where_} GROUP BY 1 {having} ORDER BY 1")
            }
            4 => format!("SELECT {first}, count(*) FROM {from}{where_} GROUP BY 1 ORDER BY 1"),
            // `IN (SELECT ...)`: semi join or hashed SubPlan.
            5 => format!("SELECT {first} FROM {from}{where_}{and_} {first} IN (SELECT id FROM b WHERE g < {m})"),
            // Correlated scalar sub-select: SubPlan.
            6 | 7 => format!("SELECT {first}, (SELECT max(v) FROM c WHERE c.b_id = {t0}.id) FROM {from}{where_}"),
            // Uncorrelated: InitPlan.
            8 => format!("SELECT {all} FROM {from}{where_}{and_} {first} > (SELECT avg(v) FROM c)"),
            9 => format!("SELECT {first} FROM {from}{where_}{and_} EXISTS (SELECT 1 FROM c WHERE c.b_id = {t0}.id AND c.v < {m})"),
            10 => format!("SELECT {first} FROM {from}{where_}{and_} NOT EXISTS (SELECT 1 FROM c WHERE c.b_id = {t0}.id)"),
            11 => format!("WITH x AS (SELECT {first} AS v FROM {from}{where_}) SELECT * FROM x x1 JOIN x x2 ON x1.v = x2.v"),
            12 => format!("WITH x AS MATERIALIZED (SELECT {first} AS v FROM {from}{where_}) SELECT count(*) FROM x WHERE v < {}", self.0.u32(1..=3000)),
            13 | 14 => {
                let all_ = *self.pick(&["", "ALL"]);
                format!("SELECT {first} FROM {from}{where_} UNION {all_} SELECT id FROM c WHERE v < {}", self.0.u32(1..=99))
            }
            15 => {
                let op = *self.pick(&["INTERSECT", "EXCEPT", "INTERSECT ALL", "EXCEPT ALL"]);
                format!("SELECT {first} FROM {from}{where_} {op} SELECT id FROM c WHERE v < {}", self.0.u32(1..=99))
            }
            16 => format!("SELECT {first}, row_number() OVER (PARTITION BY {last} ORDER BY {first}) FROM {from}{where_}"),
            17 => format!("SELECT {first}, sum({last}) OVER (ORDER BY {first} ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM {from}{where_}"),
            18 | 19 => {
                let limit = *self.pick(&[0, 1, 5, 100]);
                format!("SELECT {all} FROM {from}{where_} ORDER BY 1 LIMIT {limit}")
            }
            20 => format!("SELECT DISTINCT {first} FROM {from}{where_}"),
            // Incremental sort needs a presorted prefix.
            21 => format!("SELECT {t0}.* FROM {from}{where_} ORDER BY {t0}.{}, {t0}.id DESC LIMIT {}", TABLES[tables[0]].1[1], self.0.u32(1..=500)),
            22 => format!("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < {}) SELECT r.n, {first} FROM r JOIN {from} ON r.n = {first}{and_} true", self.0.u32(1..=200)),
            23 => format!("SELECT * FROM (VALUES (1, 'x'), ({m}, 'y'), (3, 'z')) AS v(i, s) JOIN a ON a.id = v.i"),
            24 => format!("SELECT g, {first} FROM generate_series(1, {}) g JOIN {from} ON {first} = g{and_} true", self.0.u32(1..=500)),
            25 => format!("SELECT generate_series(1, {t0}.id % {}) FROM {from}{where_}", self.0.u32(2..=5)),
            26 => format!("SELECT * FROM {} {t0} WHERE {} FOR UPDATE", TABLES[tables[0]].0, self.predicate(t0, tables[0])),
            27 => format!("SELECT * FROM a WHERE ctid = '(0,{})'", self.0.u32(1..=20)),
            28 => format!("SELECT * FROM {} WHERE ctid >= '(0,1)' AND ctid < '({},0)'", *self.pick(&["a", "b", "c"]), self.0.u32(1..=8)),
            29 => format!("SELECT * FROM a TABLESAMPLE {} ({}) REPEATABLE ({})", self.pick(&["SYSTEM", "BERNOULLI"]), self.0.u32(1..=60), self.0.u32(0..=99)),
            30 => format!("SELECT {}", self.pick(&["1", "now() > now()", "(SELECT 1)"])),
            31 => format!("SELECT * FROM a WHERE {} AND false", self.predicate("a", 0)),
            // Initial partition pruning (the bound is only known at executor start).
            32 | 33 => format!("SELECT {} FROM p WHERE id {} (SELECT {})", *self.pick(&["count(*)", "id", "g, count(*) GROUP BY g"]), *self.pick(&["<", ">", "="]), self.0.u32(0..=3000)),
            34 => format!("SELECT * FROM p ORDER BY id LIMIT {}", self.0.u32(1..=2000)),
            35 => format!("SELECT * FROM p WHERE g = {} ORDER BY id", self.0.u32(0..=4)),
            36 => format!("SELECT {t0}.id, x.m FROM {from}, LATERAL (SELECT max(c.v) AS m FROM c WHERE c.b_id = {t0}.id) x{where_}"),
            // OR of indexed columns: BitmapOr.
            37 => format!("SELECT * FROM a WHERE (k = {} OR g = {}) AND id < {}", self.0.u32(0..=99), self.0.u32(0..=6), self.0.u32(1..=3000)),
            38 => format!("SELECT * FROM a WHERE k = {} AND g = {}", self.0.u32(0..=99), self.0.u32(0..=6)),
            // Parameterised inner side with few distinct keys: Memoize.
            _ => format!("SELECT a.id, b.id FROM a JOIN b ON b.a_id = a.k WHERE a.id < {}", self.0.u32(1..=3000)),
        }
    }

    fn dml(&mut self) -> String {
        let v = self.0.u32(1..=99);
        let b = self.0.u32(1..=50);
        match self.0.u8(..9) {
            0 => format!("INSERT INTO c SELECT id + 100000, b_id, v FROM c WHERE v < {v}"),
            1 => format!("UPDATE c SET v = v + 1 WHERE b_id < {b}"),
            2 => format!("DELETE FROM c WHERE v < {v}"),
            3 => "INSERT INTO c VALUES (1, 1, 1) RETURNING *".to_owned(),
            4 => format!("UPDATE p SET id = id + 1 WHERE id < {}", self.0.u32(1..=2999)),
            5 => format!("DELETE FROM p WHERE g = {}", self.0.u32(0..=4)),
            6 => format!("UPDATE a SET k = k + 1 FROM b WHERE b.a_id = a.id AND b.g = {}", self.0.u32(0..=12)),
            7 => format!("MERGE INTO c USING b ON c.id = b.id WHEN MATCHED AND c.v < {v} THEN UPDATE SET v = c.v + 1 WHEN NOT MATCHED AND b.g = 1 THEN INSERT VALUES (b.id, b.g, b.a_id)"),
            _ => format!("INSERT INTO p SELECT id + 5000, g FROM p WHERE id < {}", self.0.u32(1..=3000)),
        }
    }
}
