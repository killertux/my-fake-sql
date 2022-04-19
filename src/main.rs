use msql_srv::*;
use std::io;
use std::net;
use std::thread;

struct Backend;
impl<W: io::Write + io::Read> MysqlShim<W> for Backend {
    type Error = io::Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }
    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        results.completed(0, 0)
    }
    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let cols = [
            Column {
                table: "foo".to_string(),
                column: "a".to_string(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "foo".to_string(),
                column: "b".to_string(),
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            },
        ];

        let mut rw = results.start(&cols)?;
        rw.write_col(42)?;
        rw.write_col("b's value")?;
        rw.finish()
    }
}

fn main() -> std::io::Result<()> {
    let listener = net::TcpListener::bind("127.0.0.1:4242")?;

    while let Ok((s, _)) = listener.accept() {
        thread::spawn(move || {
            MysqlIntermediary::run_on_tcp(Backend, s).unwrap();
        });
    }
    Ok(())
}
