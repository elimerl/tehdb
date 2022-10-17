use std::{collections::BTreeMap, fmt::Debug, fs::OpenOptions, mem::ManuallyDrop, path::Path};

use serde::{ser::SerializeSeq, Serialize, Serializer};
use smallvec::SmallVec;
use thiserror::Error;

#[derive(Clone)]
pub struct Row {
    pub elements: SmallVec<[Element; 1]>,
}

static mut CURRENT_SCHEMA: Option<Schema> = None;

impl Row {
    pub fn new(elements: SmallVec<[Element; 1]>) -> Self {
        Self { elements }
    }
}

impl Serialize for Row {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        assert_ne!(unsafe { &CURRENT_SCHEMA }, &None, "Current schema is None");

        let cur = unsafe { CURRENT_SCHEMA.as_ref() }.unwrap();
        let mut seq = serializer.serialize_seq(Some(self.elements.len()))?;
        for (i, element) in self.elements.iter().enumerate() {
            let kind = cur.elements[i].1;

            match kind {
                ElementType::I32 => {
                    let i = unsafe { element.int };
                    seq.serialize_element(&i)?;
                }
                ElementType::U32 => {
                    let i = unsafe { element.uint };
                    seq.serialize_element(&i)?;
                }
                ElementType::String => {
                    let s = unsafe { &element.string };
                    let s: &str = s.as_ref();
                    seq.serialize_element(s)?;
                }
            }
        }

        seq.end()
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub enum ElementType {
    I32,
    U32,
    String,
}

pub union Element {
    pub int: i32,
    pub uint: u32,
    pub string: ManuallyDrop<String>,
}

impl Clone for Element {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl Debug for Element {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", unsafe { self.int })
    }
}

impl From<i32> for Element {
    fn from(i: i32) -> Self {
        Element { int: i }
    }
}

impl From<u32> for Element {
    fn from(i: u32) -> Self {
        Element { uint: i }
    }
}

impl From<String> for Element {
    fn from(s: String) -> Self {
        Element {
            string: ManuallyDrop::new(s),
        }
    }
}

impl From<&str> for Element {
    fn from(s: &str) -> Self {
        Element {
            string: ManuallyDrop::new(s.to_string()),
        }
    }
}

const ROW_SIZE: usize = std::mem::size_of::<Option<Row>>();

#[derive(Clone, Serialize)]
pub struct Page {
    pub rows: BTreeMap<usize, Row>,
}

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub(crate) type DbResult<A> = Result<A, DatabaseError>;

/// A definition of the data types and keys for a set of rows.
/// ## Example
/// ```
/// // If you want to represent this struct in the database:
/// struct User {
///     id: u32,
///     email: String,
///     password: String
/// }
///
/// // You would have a schema:
/// use tehdb::database::{ElementType, Database, Row, Schema};
/// let schema = Schema::new(vec![("id".into(), ElementType::U32), ("email".into(), ElementType::String), ("password".into(), ElementType::String)], /* primary key */ 0);
///
/// // So you can create a database:
/// let mut db = Database::new(schema);
///
/// // Then you can insert stuff into:
/// # let user = User {id:0,email:"mark@fb.com".into(),password:"password123".into()};
/// # use smallvec::smallvec;
/// db.insert(Row::new(smallvec![user.id.into(), user.email.into(), user.password.into()]));
/// ```
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Schema {
    pub elements: Vec<(String, ElementType)>,
    /// The primary key index.
    pub pk: usize,
}

impl Schema {
    pub fn new(elements: Vec<(String, ElementType)>, pk: usize) -> Self {
        if elements[pk].1 != ElementType::U32 && elements[pk].1 != ElementType::I32 {
            panic!(
                "Primary key type is {:?}; must be U32 or I32",
                elements[pk].1
            );
        }
        Self { elements, pk }
    }
    /// Does this row match the schema?
    /// This function does not really determine if a row matches the schema; it only checks the lengths of elements.
    /// This is because rows are untagged, so it can't really tell what type an element is.
    pub fn is_match(&self, row: &Row) -> bool {
        row.elements.len() == self.elements.len()
    }

    pub fn get_type(&self, idx: usize) -> ElementType {
        self.elements[idx].1
    }

    pub fn get_pk_int(&self, row: &Row) -> i32 {
        assert_eq!(self.elements[self.pk].1, ElementType::I32);
        unsafe { row.elements[self.pk].int }
    }

    pub fn get_pk_uint(&self, row: &Row) -> u32 {
        assert_eq!(self.elements[self.pk].1, ElementType::U32);
        unsafe { row.elements[self.pk].uint }
    }

    pub fn try_get_int(&self, row: &Row, idx: usize) -> Option<i32> {
        if self.elements[idx].1 != ElementType::I32 {
            return None;
        }
        Some(unsafe { row.elements[idx].int })
    }

    pub fn try_get_uint(&self, row: &Row, idx: usize) -> Option<u32> {
        if self.elements[idx].1 != ElementType::U32 {
            return None;
        }
        Some(unsafe { row.elements[idx].uint })
    }

    pub fn try_get_str<'a>(&'a self, row: &'a Row, idx: usize) -> Option<&str> {
        if self.elements[idx].1 != ElementType::String {
            return None;
        }
        Some(unsafe { &row.elements[idx].string })
    }

    pub fn get_int(&self, row: &Row, idx: usize) -> i32 {
        self.try_get_int(row, idx).unwrap()
    }

    pub fn get_uint(&self, row: &Row, idx: usize) -> u32 {
        self.try_get_uint(row, idx).unwrap()
    }

    pub fn get_str<'a>(&'a self, row: &'a Row, idx: usize) -> &str {
        self.try_get_str(row, idx).unwrap()
    }
}

#[derive(Clone)]
pub struct Database {
    num_rows: usize,
    pages: [Option<Box<Page>>; TABLE_MAX_PAGES],
    schema: Schema,
}

impl Drop for Database {
    fn drop(&mut self) {
        let schema = &self.schema;

        for page in self.pages.iter_mut().flatten() {
            for row in page.rows.values_mut() {
                for (idx, elem) in row.elements.iter_mut().enumerate() {
                    if schema.elements[idx].1 == ElementType::String {
                        unsafe {
                            ManuallyDrop::<String>::drop(&mut elem.string);
                        }
                    }
                }
            }
        }
    }
}

impl Database {
    pub fn new(schema: Schema) -> Self {
        pub(crate) const PAGE_INIT: Option<Box<Page>> = None;

        Self {
            num_rows: 0,
            schema,
            pages: [PAGE_INIT; TABLE_MAX_PAGES],
        }
    }
    /// Gets or creates a page. If the index is out of bounds, returns None.
    pub fn get_or_create_page(&mut self, idx: usize) -> Option<&mut Page> {
        if idx >= TABLE_MAX_PAGES {
            return None;
        }
        let page = &mut self.pages[idx];
        if let Some(s) = page {
            Some(s)
        } else {
            *page = Some(Box::new(Page {
                rows: BTreeMap::new(),
            }));
            Some(page.as_mut().unwrap())
        }
    }
    pub fn rows(&self) -> usize {
        self.num_rows
    }
    pub fn max_rows(&self) -> usize {
        TABLE_MAX_ROWS
    }

    pub fn insert(&mut self, row: Row) -> DbResult<()> {
        if self.num_rows > self.max_rows() {
            return Err(DatabaseError::OutOfRows);
        }

        let new_row_idx = self.num_rows;
        let page_idx = new_row_idx / ROWS_PER_PAGE;
        if self.schema.get_type(self.schema.pk) == ElementType::I32 {
            let id = self.schema.get_pk_int(&row);

            self.get_or_create_page(page_idx)
                .ok_or(DatabaseError::OutOfRows)?
                .rows
                .insert(id as usize, row);
        } else {
            let id = self.schema.get_pk_uint(&row);

            self.get_or_create_page(page_idx)
                .ok_or(DatabaseError::OutOfRows)?
                .rows
                .insert(id as usize, row);
        }
        self.num_rows += 1;

        Ok(())
    }

    pub fn select_by_pk(&self, pk: u32) -> Option<&Row> {
        self.pages
            .iter()
            .find(|e| {
                if let Some(page) = e {
                    page.rows.get(&(pk as usize)).is_some()
                } else {
                    false
                }
            })
            .map(|e| {
                if let Some(page) = e {
                    page.rows.get(&(pk as usize)).unwrap()
                } else {
                    unreachable!()
                }
            })
    }

    /// Saves this database to a directory. Creates it if it doesn't already exist.
    pub fn save_to_directory<P: AsRef<Path>>(&self, directory: P) -> std::io::Result<()> {
        if !directory.as_ref().exists() {
            std::fs::create_dir(&directory)?;
        }

        unsafe {
            CURRENT_SCHEMA = Some(self.schema.clone());
        }

        for page in 0..self.pages.len() {
            if self.pages[page].is_none() {
                return Ok(());
            }
            let filename = format!("page{}.json", page);
            let writer = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(directory.as_ref().join(filename))?;
            serde_json::to_writer(writer, &self.pages[page]).unwrap();
        }

        unsafe {
            CURRENT_SCHEMA = None;
        }

        Ok(())
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum DatabaseError {
    #[error("no more space for new rows")]
    OutOfRows,

    #[error("row did not match schema")]
    DidNotMatchSchema,

    #[error("unknown error")]
    Unknown,
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;

    use crate::database::{DatabaseError, ElementType, Schema};

    use super::{Database, Row};

    #[test]
    fn insert_single() {
        let row = Row::new(smallvec![0.into()]);
        let mut db = Database::new(Schema::new(vec![("id".into(), ElementType::I32)], 0));

        db.insert(row).unwrap();

        assert_eq!(db.rows(), 1);
        let got_row = db.select_by_pk(0).unwrap();
        assert_eq!(db.schema.get_int(got_row, 0), 0);
    }

    #[test]
    fn insert_single_with_string() {
        let row = Row::new(smallvec![0.into(), "hello, world".into()]);
        let mut db = Database::new(Schema::new(
            vec![
                ("id".into(), ElementType::I32),
                ("name".into(), ElementType::String),
            ],
            0,
        ));

        db.insert(row).unwrap();

        assert_eq!(db.rows(), 1);

        let got_row = db.select_by_pk(0).unwrap();
        println!("{:?}", db.schema);

        assert_eq!(db.schema.get_int(got_row, 0), 0);
        assert_eq!(db.schema.get_str(got_row, 1), "hello, world");
    }

    #[test]
    fn insert_ten() {
        let mut db = Database::new(Schema::new(vec![("id".into(), ElementType::I32)], 0));

        for i in 0..10 {
            let row = Row::new(smallvec![i.into()]);

            db.insert(row).unwrap();
        }

        assert_eq!(db.rows(), 10);
    }

    #[test]
    fn insert_max() {
        let mut db = Database::new(Schema::new(vec![("id".into(), ElementType::I32)], 0));
        for i in 0..db.max_rows() as u32 {
            let row = Row::new(smallvec![i.into()]);

            db.insert(row).unwrap();
        }

        assert_eq!(db.rows(), db.max_rows());
    }

    #[test]
    fn insert_above_max() {
        let mut db = Database::new(Schema::new(vec![("id".into(), ElementType::I32)], 0));

        for i in 0..(db.max_rows() as u32) {
            let row = Row::new(smallvec![i.into()]);

            db.insert(row).unwrap();
        }

        let row = Row::new(smallvec![((db.max_rows() + 1) as i32).into()]);
        assert_eq!(db.insert(row), Err(DatabaseError::OutOfRows));
        assert_eq!(db.rows(), db.max_rows());
    }
    #[test]
    fn save_load_single() {
        let mut db = Database::new(Schema::new(vec![("id".into(), ElementType::I32)], 0));

        db.insert(Row::new(smallvec![0.into()])).unwrap();
        let tempdir = tempfile::tempdir().unwrap();

        db.save_to_directory(tempdir.path().join("db")).unwrap();
    }
    #[test]
    fn save_load_1k() {
        let mut db = Database::new(Schema::new(
            vec![
                ("id".into(), ElementType::I32),
                ("name".into(), ElementType::String),
            ],
            0,
        ));

        for i in 0..1000 {
            let row = Row::new(smallvec![i.into(), "Hello world".into()]);

            db.insert(row).unwrap();
        }

        let tempdir = tempfile::tempdir().unwrap();

        db.save_to_directory(tempdir.path().join("db")).unwrap();
    }
}
