use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use rayon::prelude::*;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let before = Instant::now();
    let file_path = String::from("test_dataset.parquet");

    let sum = process_parquet(file_path);
    println!("Sum: {:?}", sum);
    println!("Total execution time: {:.2?}", before.elapsed());

    Ok(())
}

fn process_parquet(file_path: String) -> i32 {
    let file = File::open(file_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let iter = reader.get_row_iter(None).unwrap();

    // Map each row to a partial sum and then reduce them
    let partial_sums: Vec<i32> = iter
        .flatten()
        .par_bridge()
        .map(|record| {
            let mut partial_sum = 0;
            let columns = record.get_column_iter().collect::<Vec<_>>();
            for (idx, (_, value)) in columns.iter().enumerate() {
                match value {
                    Field::Str(value) if value == "a" && idx + 1 < columns.len() => {
                        if let Field::Str(val) = &columns[idx + 1].1 {
                            if let Ok(number) = val.parse::<i32>() {
                                partial_sum += number;
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
            partial_sum
        })
        .collect();

    // Sum up all the partial sums
    partial_sums.iter().sum()

    // let file = File::open(file_path).unwrap();
    // let new_reader = SerializedFileReader::new(file).unwrap();

    // let new_iter = new_reader.get_row_iter(None).unwrap();

    // let mut rolling_sum: i32 = 0;

    // for record in new_iter.flatten() {
    //     let columns = record.get_column_iter().collect::<Vec<_>>();
    //     for (idx, (_, value)) in columns.iter().enumerate() {
    //         match value {
    //             parquet::record::Field::Str(value) if value == "a" => {
    //                 // Now safely access the next field
    //                 let (_, next_value) = columns[idx + 1];
    //                 // println!("Next field value: {:?}", next_value);
    //                 match next_value {
    //                     parquet::record::Field::Str(val) => {
    //                         let v: Result<i32, _> = val.parse();
    //                         match v {
    //                             Ok(number) => {
    //                                 rolling_sum += number;
    //                                 // println!("Found!: Sum now = {:?}", rolling_sum);
    //                                 break;
    //                             }
    //                             Err(_) => println!("Err"),
    //                         }
    //                     }
    //                     _ => println!("Whoops"),
    //                 }
    //             }

    //             _ => {} // Handle other cases or do nothing
    //         }
    //     }
    // }
    // return rolling_sum;
}
