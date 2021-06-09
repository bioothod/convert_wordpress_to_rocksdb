use crypto::digest::Digest;
use crypto::sha3::Sha3;
use mysql::*;
use mysql::prelude::*;
use rocksdb;
use std::fs;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Cli {
    #[structopt(short="p", long="password_file")]
    password_file: String,

    #[structopt(short="u", long="user", default_value="s0mbre")]
    user: String,

    #[structopt(short="d", long="db_name", default_value="ioremap_net_wp")]
    db_name: String,

    #[structopt(short="P", long="port", default_value="3306")]
    port: u32,

    #[structopt(short="H", long="host", default_value="localhost")]
    host: String,
    
    #[structopt(short="o", long="output_db", parse(from_os_str))]
    output_db: std::path::PathBuf,
}

struct Post {
    id: u32,
    date_str: String,
    date: mysql::chrono::NaiveDateTime,
    content: String,
    title: String,
}

fn get_posts(user: String, password: String, host: String, port: u32, db_name: String, table_names: Vec<&str>) -> Result<Vec<Post>> {
    let url = format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, db_name);
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;

    let mut posts = Vec::<Post>::new();

    for table_name in table_names.iter() {
        conn.query_map(
            format!("SELECT ID, post_date, post_content, post_title from {}", table_name),
            |(id, date, content, title)| {
                let title = from_value::<String>(title);
                let date_str = from_value::<String>(date);
                let date = match mysql::chrono::NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S") {
                    Ok(date) => date,
                    Err(err) => {
                        println!("could not parse date: {}: '{}': {:?}", date_str, title, err);
                        mysql::chrono::NaiveDateTime::from_timestamp(0, 0)
                    },
                };

                let post = Post {
                    id: from_value::<u32>(id),
                    date_str: date_str,
                    date: date,
                    content: from_value::<String>(content),
                    title: title,
                };

                posts.push(post);
            },
        )?;
    }

    Ok(posts)
}

fn main() {
    let args = Cli::from_args();

    let mut password = fs::read_to_string(args.password_file.to_owned())
        .expect(&format!("Something went wrong reading the file '{}'", args.password_file).to_owned());

    if password.ends_with("\n") {
        password.remove(password.len()-1);
    }

    let tables = vec!["wp_posts", "wpnews_posts"];
    let mut posts = match get_posts(args.user, password, args.host, args.port, args.db_name, tables) {
        Err(err) => panic!("could not read posts: {}", err),
        Ok(posts) => posts,
    };

    println!("posts: {}", posts.len());

    let mut opts = rocksdb::Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.increase_parallelism(4);
    opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
    opts.optimize_for_point_lookup(1024);
    opts.set_bytes_per_sync(1024 * 1024);


    let db = rocksdb::DB::open(&opts, args.output_db).unwrap();

    //let mut hasher = Sha3::sha3_256();
    //let hash: &mut [u8] = &mut vec![0; hasher.output_bytes()];

    for (i, p) in posts.iter_mut().enumerate() {
        //hasher.input_str(&p.content);
        //hasher.result(hash);
        //hasher.reset();

        db.put(&p.date.timestamp().to_le_bytes(), &p.content).expect("could not write post entry");
    }

    let x: Option<&[u8]> = None;
    db.compact_range(x, x);
}
