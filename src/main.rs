use sqlx::{mysql::{MySqlPoolOptions, MySqlRow}, MySql, Pool, Row};
use dotenvy::dotenv;
use std::env;
use plotters::prelude::*;
use plotters::coord::Shift;

const TOP_MARGIN: i32 = 60;
const BOTTOM_MARGIN: i32 = 40;
const LEFT_MARGIN: i32 = 120;
const RIGHT_MARGIN: i32 = 40;
const DWG_WIDTH: i32 = 1280; //this is overall size of image
const DWG_HEIGHT: i32 = 790; // approximately golden ratio
const AXIS_WIDTH: i32 = DWG_WIDTH - LEFT_MARGIN - RIGHT_MARGIN;
const AXIS_HEIGHT: i32 = DWG_HEIGHT - TOP_MARGIN - BOTTOM_MARGIN;
const H_TICK_WIDTH: i32 = (AXIS_WIDTH) / 4;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // Load environment variables from a .env file (if needed)
    dotenv().ok();

    // Set up the database URL from environment variable
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Create a connection pool
    let pool: Pool<MySql> = MySqlPoolOptions::new()
        .max_connections(5) // Set the maximum number of connections
        .connect(&database_url)
        .await?;


    let period = "Month"; // options are "Week", "Fort", "Month"
    let city  = "Phoenix_AZ"; //SQL ignores upper/lower case for table names & in name_of_city column
    let city_period = format!("{city}_{period}");
    let span = "tmonth"; // could be tmonth, tfort, or tweek
    let year = 2024;
    let city_low; // must be fn_main scope so just declare it here
    let city_high; // must be fn_main scope so just declare it here

    let fn_get_min_max: Result<(i32, i32), sqlx::Error> = get_city_min_max(&pool, city).await;
    match fn_get_min_max { // city_low & city_high here must be initialized in this block to make compiler happy
        Ok(_) => {let min_max: &(i32, i32) = &fn_get_min_max.unwrap();
                city_low = min_max.0; 
                city_high = min_max.1;
                println!("Low: {city_low}  High: {city_high}")},
        Err(e) =>  {city_low = 0; city_high = 0; println!("Error getting City min max: {}",e)}
    }  

    let y_range = (city_high + 5) - (city_low - 10);
    let pixel_per_degree: f64 = f64::from(AXIS_HEIGHT) / f64::from(y_range);
    println!("Axis Height: {AXIS_HEIGHT} Y range: {y_range} Pixels per degree: {pixel_per_degree}");
    // should calc max temperature range and how many pixels per degree here = axis_height / (city_high - city_low)

    let title_style = ("sans-serif", 36).into_font().color(&BLACK);
    let title_text = format!("{year} {city}  {period} Avg Temperatures");
    let x_axis_style = ("sans-serif", 14).into_font().color(&BLACK);
    let y_axis_style = ("sans-serif", 18).into_font().color(&BLACK);

    let dwg = BitMapBackend::new("imgs/100-2024.png", (DWG_WIDTH as u32, DWG_HEIGHT as u32)).into_drawing_area();
    dwg.fill(&WHITE).expect("Failed to fill dwg"); //this automatically makes a rectangle size of drawing area and fills it with white

    // Draw axis lines on the drawing area
    draw_axes(&dwg).expect("Failed to draw axes");
    
    // Draw horizontal and verticlal grid lines with tick marks
    draw_grids(&dwg).expect("Failed to draw grids");

    // Draw title
    draw_title(&dwg, &title_text, title_style).expect("Failed to draw title");

    // Draw axis labels
    draw_axis_labels(&dwg, x_axis_style, y_axis_style, period, city_low, city_high).expect("Failed to draw axis labels");

    
    let fn_result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> = get_temps(&pool, span, &city_period, year).await;
    match fn_result {
        Ok(_) => { 
            print_avgs(span, &city_period, year, &fn_result.as_ref().unwrap());
            draw_hi_temps(&dwg, period, pixel_per_degree, &fn_result.as_ref().unwrap()).expect("Draw Hi Temps Failed"); 
            draw_low_temps(&dwg, period, pixel_per_degree, &fn_result.as_ref().unwrap()).expect("Draw Low Temps Failed");
        }
        Err(e) => eprintln!("Error getting temperatures from db: {}", e),
    }

    dwg.present().expect("Failed Chart drawing");

    Ok(())
}

fn draw_hi_temps(dwg: &DrawingArea<BitMapBackend, Shift>, period: &str, pixel_per_degree: f64, rows: &Vec<MySqlRow>) -> Result<(), Box<dyn std::error::Error>> {
    //if rows.is_empty() {
        //println!("No high temperature data found");
    //    Err(e) =>  println!("Error getting temp data: {}",e),
    //}
    match period {
        "Week" => {    
            for i in 1..53 {
                let x = i * (AXIS_WIDTH / 52) + LEFT_MARGIN;
                dwg.draw(&Rectangle::new(
                    [(x, AXIS_HEIGHT + TOP_MARGIN -3), (x+8, AXIS_HEIGHT - i * 10)],
                    Into::<ShapeStyle>::into(&RED).filled(),
                ))?
            }   
        },
        "Fort" => {    
            for i in 1..27 {
                let x = i * (AXIS_WIDTH / 26) + LEFT_MARGIN - 10;
                dwg.draw(&Rectangle::new(
                    [(x, AXIS_HEIGHT + TOP_MARGIN -3), (x+18, AXIS_HEIGHT - i * 20)],
                    Into::<ShapeStyle>::into(&RED).filled(),
                ))?
            }   
        },
        "Month" => {
            for i in 1..13 {
                let x = i * (AXIS_WIDTH / 12) + LEFT_MARGIN - 50;
                let idx: usize = i.try_into().unwrap();
                let tmp: i32 = rows[idx-1].get(2);
                let y: f64 = f64::from(tmp) * pixel_per_degree;
                //print!("{y}, ");
                dwg.draw(&Rectangle::new(
                    [(x, AXIS_HEIGHT + TOP_MARGIN -3), (x+30, DWG_HEIGHT  - (y.round() as i32))],
                    Into::<ShapeStyle>::into(&RED).filled(),
                ))?;
            }
        },
        _ => println!("Unknown Period"),
    }
    Ok(())
}

fn draw_low_temps(dwg: &DrawingArea<BitMapBackend, Shift>, period: &str, pixel_per_degree: f64, rows: &Vec<MySqlRow>) -> Result<(), Box<dyn std::error::Error>>  {
    //if rows.is_empty() {
    //    println!("No low temperature data found");
    //    Err(error)
    // }
    match period {
        "Week" => {
            for i in 1..53 {
                let x = i * (AXIS_WIDTH / 52) +  LEFT_MARGIN;
                dwg.draw(&Rectangle::new(
                    [(x, AXIS_HEIGHT + TOP_MARGIN -3), (x+8, AXIS_HEIGHT - i *7)],
                    Into::<ShapeStyle>::into(&GREEN).filled(),
                ))?
            }
        },
        "Fort" => {
            for i in 1..27 {
                let x = i * (AXIS_WIDTH / 26) +  LEFT_MARGIN - 10;
                dwg.draw(&Rectangle::new(
                    [(x, AXIS_HEIGHT + TOP_MARGIN -3), (x+18, AXIS_HEIGHT - i *7)],
                    Into::<ShapeStyle>::into(&GREEN).filled(),
                ))?
            }
        },
        "Month" => {
            for i in 1..13 {
                let x = i * (AXIS_WIDTH / 12) + LEFT_MARGIN - 50;
                let idx: usize = i.try_into().unwrap();
                let tmp: i32 = rows[idx-1].get(3);
                let y: f64 = f64::from(tmp) * pixel_per_degree;
                //print!("l{y}, ");
                dwg.draw(&Rectangle::new(
                    [(x, AXIS_HEIGHT + TOP_MARGIN -3), (x+30, DWG_HEIGHT  - (y.round() as i32))],
                    Into::<ShapeStyle>::into(&GREEN).filled(),
                ))?;
            }
        },
        _ => println!("Unknown Period"),
    }
    Ok(())    
}

fn draw_axes(dwg: &DrawingArea<BitMapBackend, Shift>) -> Result<(), Box<dyn std::error::Error>> {
    // Draw axis lines on the drawing area
    dwg.draw(&PathElement::new( //draw y axis
        vec![(LEFT_MARGIN, TOP_MARGIN), (LEFT_MARGIN, AXIS_HEIGHT + TOP_MARGIN)],
        Into::<ShapeStyle>::into(&BLACK).stroke_width(5),
    ))?;    
    dwg.draw(&PathElement::new( //draw x axis
        vec![(LEFT_MARGIN-2, AXIS_HEIGHT + TOP_MARGIN), (AXIS_WIDTH + LEFT_MARGIN, AXIS_HEIGHT + TOP_MARGIN)],
        Into::<ShapeStyle>::into(&BLACK).stroke_width(5),
    ))?;  
    Ok(())  
}

fn draw_grids(dwg: &DrawingArea<BitMapBackend, Shift>) -> Result<(), Box<dyn std::error::Error>> {
    // Draw 4 vertical grid lines
    for i in 1..5 { 
        let x = LEFT_MARGIN + i * H_TICK_WIDTH;
        dwg.draw(&PathElement::new(  //draw vertical grid line
            vec![(x, TOP_MARGIN), (x, AXIS_HEIGHT + TOP_MARGIN-2)],
            Into::<ShapeStyle>::into(RGBColor(128, 128, 128)).stroke_width(1),
        ))?;
        dwg.draw(&PathElement::new(  //draw tick mark on x axis
            vec![(x, AXIS_HEIGHT + TOP_MARGIN ), (x, AXIS_HEIGHT + TOP_MARGIN+10)],
            Into::<ShapeStyle>::into(&BLACK).stroke_width(3),
        ))?;
    }
    // Draw 10 horizontal grid lines
    for i in 0..10 {
        let y = TOP_MARGIN + i * AXIS_HEIGHT / 10;
        dwg.draw(&PathElement::new(
            vec![(LEFT_MARGIN+2, y), (AXIS_WIDTH + LEFT_MARGIN, y)],
            Into::<ShapeStyle>::into(RGBColor(128, 128, 128)).stroke_width(1),
        ))?;
        dwg.draw(&PathElement::new(  //draw tick mark on y axis
            vec![(LEFT_MARGIN -10, y), (LEFT_MARGIN , y)],
            Into::<ShapeStyle>::into(&BLACK).stroke_width(3),
        ))?;
    }
    Ok(())
}

fn draw_title(dwg: &DrawingArea<BitMapBackend, Shift>, title_text: &str, title_style: TextStyle) -> Result<(), Box<dyn std::error::Error>> {
    let (title_width, title_height) = dwg.estimate_text_size(&title_text, &title_style)?;
    
    dwg.draw_text(&title_text, &title_style,
        ((DWG_WIDTH / 2) as i32 - (title_width as i32 / 2), title_height as i32 - 10),
    )?; 
    Ok(())
}

fn draw_axis_labels(dwg: &DrawingArea<BitMapBackend, Shift>,
                         x_axis_style: TextStyle, 
                         y_axis_style: TextStyle, 
                         period: &str,
                         city_low: i32,
                         city_high: i32) -> Result<(), Box<dyn std::error::Error>> {
    match period {
        "Week" => {
            let (_x_label_width, x_label_height) = dwg.estimate_text_size(&format!("55"), &x_axis_style)?;
            //println!("x_label_width: {}, x_label_height: {}", _x_label_width, x_label_height);
            for i in 1..53 {
                let x = i * (AXIS_WIDTH / 52) + LEFT_MARGIN;
                let i_str = i.to_string();
                dwg.draw_text(&i_str, &x_axis_style, (x - 1, AXIS_HEIGHT + TOP_MARGIN + (x_label_height / 2) as i32 + 5))?;
            }
        },
        "Fort" => {
            let (_x_label_width, x_label_height) = dwg.estimate_text_size(&format!("55"), &x_axis_style)?;
            //println!("x_label_width: {}, x_label_height: {}", _x_label_width, x_label_height);
            for i in 1..27 {
                let x = i * (AXIS_WIDTH / 26) + LEFT_MARGIN - 5;
                let i_str = i.to_string();
                dwg.draw_text(&i_str, &x_axis_style, (x - 1, AXIS_HEIGHT + TOP_MARGIN + (x_label_height / 2) as i32 + 5))?;
            }
        },
        "Month" =>  {   
            for i   in 1..13 {
                let month_abbr = match i {
                    1 => "Jan",
                    2 => "Feb",
                    3 => "Mar",
                    4 => "Apr",
                    5 => "May",
                    6 => "Jun",
                    7 => "Jul",
                    8 => "Aug",
                    9 => "Sep",
                    10 => "Oct",
                    11 => "Nov",
                    12 => "Dec",
                    _ => "",
                };
                let x = i * (AXIS_WIDTH / 12) + LEFT_MARGIN - 45;
                dwg.draw_text(&month_abbr, &x_axis_style, (x, AXIS_HEIGHT + TOP_MARGIN + 10))?;
            }
        },
        _ => println!("Unknown Period"),
    }

    // Draw Y Axis Label
    let (y_label_width, y_label_height) = dwg.estimate_text_size(&format!("{}", city_high), &y_axis_style)?;
    //println!("y_label_width: {}, y_label_height: {}", y_label_width, y_label_height);
    let max_temp: i32 = city_high + 5; //replace with fn to round up to even 5's or 10's
    let min_temp: i32 = city_low - 5;   //replace with fn to round down to even 5's or 10's
    // min temp must handle negative temps correctly
    let temp_range:i32 = max_temp - min_temp;
    let temp: f64 = temp_range as f64 / 10.0;
    let tenth_range: i32 = temp.round() as i32; // amount to adjust for each horizontal grid line
    for i in 0..10 {
        let y = TOP_MARGIN + i * AXIS_HEIGHT / 10;
        let i_str = (max_temp - tenth_range * i).to_string();
        dwg.draw_text(&i_str, &y_axis_style, (LEFT_MARGIN - 12- y_label_width as i32, y - (y_label_height/2) as i32))?;
    }
    Ok(())
}
async fn get_city_min_max(pool: &Pool<MySql>, city: &str) -> Result<(i32, i32), sqlx::Error> {
    // Query data from the table
    let query_string = format!("SELECT min_temp, max_temp FROM city_names WHERE name_of_city = '{}'", city); // Adjust table name as needed
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_string)
        .fetch_all(pool)
        .await?; // had to make this function return a Result to use the ? operator

    let lo: i32 = rows[0].get(0);
    let hi: i32 = rows[0].get(1);

    Ok((lo, hi))
}

async fn get_temps(pool: &Pool<MySql>, span: &str, city: &str, year: i32) -> Result<Vec<MySqlRow>, sqlx::Error> {
    // Query data from the table
    let query_string = format!("SELECT tyear, {}, tmax, tmin FROM {} WHERE tyear = {}", span, city, year ); // Adjust table name as needed
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_string)
        .fetch_all(pool)
        .await?; // had to make this function return a Result to use the ? operator
    Ok(rows)
}

fn print_avgs(span: &str, city: &str, year: i32, rows: &Vec<MySqlRow>) {
    if rows.is_empty() {
        println!("No {} data found for {} in {}", span, city, year);
        return;
    }
    println!("Avg {} temps for {} in {}", span, city, year);
    for row in rows {
        let year: i32 = row.get("tyear");
        let week: i32 = row.get(1); // use index instead of tmonth/tfort/tweek
        let hi_temp: i32 = row.get("tmax");
        let lo_temp: i32 = row.get("tmin");
        println!("{}-{}: Avg Hi={}, Avg Lo={}", year, week, hi_temp, lo_temp);
    }
}
