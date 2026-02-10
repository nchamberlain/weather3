use sqlx::{mysql::{MySqlPoolOptions, MySqlRow}, MySql, Pool, Row};
use dotenvy::dotenv;
use std::env;
use plotters::prelude::*;
use plotters::coord::Shift;

const TOP_MARGIN: i32 = 60;
const BOTTOM_MARGIN: i32 = 40;
const LEFT_MARGIN: i32 = 70;
const RIGHT_MARGIN: i32 = 40;
const DWG_WIDTH: i32 = 1280; //this is overall size of image
const DWG_HEIGHT: i32 = 800; // approximately golden ratio
const AXIS_WIDTH: i32 = DWG_WIDTH - LEFT_MARGIN - RIGHT_MARGIN;
const AXIS_HEIGHT: i32 = DWG_HEIGHT - TOP_MARGIN - BOTTOM_MARGIN;
const H_TICK_WIDTH: i32 = AXIS_WIDTH / 4;
const V_TICK_HEIGHT: i32 = AXIS_HEIGHT / 10;
const TOP_LINE_Y: i32 = 0 + TOP_MARGIN; //x height of top line of chart, might NOT = TOP_MARGIN
const BOTTOM_LINE_Y: i32 = TOP_LINE_Y + AXIS_HEIGHT;

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
    let city  = "Los_Angeles_CA"; //SQL ignores upper/lower case for table names & in name_of_city column
    let city_period = format!("{city}_{period}");
    let tperiod = "tmonth"; // column names in selected db: can be tmonth, tfort, or tweek
    let mut first_year = 1899; // using a date before 20th century make sure earliest date for that city is used
    let mut last_year = 2030; // using a future date makes sure the latest valid date for that city is used

    let city_low; // must be fn_main scope so just declare it here
    let city_high; // must be fn_main scope so just declare it here
    
    let fn_get_min_max: Result<(i32, i32), sqlx::Error> = get_city_min_max(&pool, city).await;
    match fn_get_min_max { // city_low & city_high here must be initialized in this block to make compiler happy
        Ok(_) => {let min_max: &(i32, i32) = &fn_get_min_max.unwrap();
                city_low = min_max.0; 
                city_high = min_max.1;
                println!("Low: {city_low}  High: {city_high}")},
        Err(e) =>  {city_low = 0; city_high = 0; eprintln!("Error getting City min max: {}",e)}
    }  

    let first_year_result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> = get_first_year(&pool, city).await;
    match first_year_result {
        Ok(_) => { 
            let first_year_row = &first_year_result.unwrap(); //unwrap the row
            let first_year_str: &str = first_year_row[0].get("tdate"); //get date string, for ex. 2020-09-05
            println!("First year for {}: {}", city, &first_year_str[0..4]);
            let first_year_int: i32 = first_year_str[0..4].parse().unwrap();  //parse first 4 digits as an int
            if first_year_int > first_year {                                  //make sure last year is valid for that city
                first_year = first_year_int;
            }
        },
        Err(e) => eprintln!("Error executing function: {}", e),
    } 

    let last_year_result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> = get_last_year(&pool, city).await;
    match last_year_result {
        Ok(_) => { 
            let last_year_row = &last_year_result.unwrap(); //unwrap the row
            let last_year_str: &str = last_year_row[0].get("tdate"); //get date string, for ex. 2020-11-21
            let last_year_int: i32 = last_year_str[0..4].parse().unwrap();  //parse first 4 digits as an int
            if last_year > last_year_int {                                  //make sure last year is valid for that city
                last_year = last_year_int;
            }
            println!("Last year for {}: {}", city, last_year);
        },
        Err(e) => eprintln!("Error executing function: {}", e),
    } 

    let file_name = format!("imgs/{city}_{first_year}_{period}.png");

    // calc these here so available to the functions
    let y_lowest = city_low-10;
    let y_highest = city_high + 5;
    let y_range =  y_highest - y_lowest; //neg y_lowest increases y_range
    let pixel_per_degree: f64 = f64::from(AXIS_HEIGHT) / f64::from(y_range);
    let mut zero_line_offset = 0.0;
    if y_lowest < 0  { 
        zero_line_offset = (f64::from(y_lowest) * pixel_per_degree).abs(); //
    } else if y_lowest == 0 {
        zero_line_offset = 0.0;
    } else {
        let z_diff = 0 - y_lowest -1;
        zero_line_offset = f64::from(z_diff) * pixel_per_degree;
    }
    println!("Axis Height: {AXIS_HEIGHT} Y range: {y_range} degrees. Pixels per degree: {pixel_per_degree}. Zero offset: {zero_line_offset}");

    let title_period; 
    match period {
        "Week" => title_period = "Weekly",
        "Fort" => title_period = "Fortnightly",
        "Month" => title_period = "Montly",
        _ => title_period = "Unknown Period",
    }
    let title_text = format!("{first_year} {city}  {title_period} Avg Temperatures");
    let title_style = ("sans-serif", 36).into_font().color(&BLACK);
    let x_axis_style = ("sans-serif", 14).into_font().color(&BLACK);
    let y_axis_style = ("sans-serif", 18).into_font().color(&BLACK);

    let dwg = BitMapBackend::new(&file_name, (DWG_WIDTH as u32, DWG_HEIGHT as u32)).into_drawing_area();
    dwg.fill(&WHITE).expect("Failed to fill dwg"); //this automatically makes a rectangle size of drawing area and fills it with white

    // Draw axis lines on the drawing area
    draw_axes(&dwg).expect("Failed to draw axes");
    
    // Draw horizontal and verticlal grid lines with tick marks
    draw_grids(&dwg).expect("Failed to draw grids");

    // Draw title
    draw_title(&dwg, &title_text, title_style).expect("Failed to draw title");

    // Draw axis labels
    draw_axis_labels(&dwg, x_axis_style, y_axis_style, period, y_lowest, y_highest, y_range).expect("Failed to draw axis labels");

    
    let fn_result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> = get_temps(&pool, tperiod, &city_period, first_year).await;
    match fn_result {
        Ok(_) => { 
            print_avgs(period, &city_period, first_year, &fn_result.as_ref().unwrap());
            draw_hi_temps(&dwg, period, zero_line_offset, pixel_per_degree, &fn_result.as_ref().unwrap()).expect("Draw Hi Temps Failed"); 
            draw_low_temps(&dwg, period, zero_line_offset, pixel_per_degree, &fn_result.as_ref().unwrap()).expect("Draw Low Temps Failed");
        }
        Err(e) => eprintln!("Error getting temperatures from db: {}", e),
    }
     //the following functions seem to work fine and don't need more testing
    /*let city_list_result: Result<Vec<MySqlRow>, sqlx::Error> = list_cities(&pool).await;
    match city_list_result {
        Ok(_) => { //probably only returns Ok if it found something. otherwise it would return err, no empty check
            let city_list = city_list_result.unwrap();
            for a_city in city_list {
                let c_name: &str = a_city.get("name_of_city");
                println!("Available city: {c_name}");
            }
        },
        Err(e) => eprint!("Cities not found, {} ", e),
    }
    let city_to_drop = "spokane_wa";
    let drop_city_subs_result = drop_city_sub_tables(&pool, city_to_drop).await;
    match drop_city_subs_result {
        Ok(_) => println!("Dropped sub tables for {city}"),
        Err(e) => eprint!("Error dropping subtables: {e}"),
    }

    let city_to_create = "spokane_wa";
    let create_city_subs_result = create_city_sub_tables(&pool, city_to_create).await;
    match create_city_subs_result {
        Ok(_) => println!("Created sub tables for {city}"),
        Err(e) => eprint!("Error creating subtables: {e}"),
    }
*/
    dwg.present().expect("Failed Chart drawing");

    Ok(())
}
// ======================================================

fn draw_hi_temps(dwg: &DrawingArea<BitMapBackend, Shift>, period: &str, z_line_offset: f64,  pixel_per_degree: f64, rows: &Vec<MySqlRow>) -> Result<(), Box<dyn std::error::Error>> {
    let mut y_adj: i32;
    match period {
        "Week" => {    
            for i in 1..53 {
                let x = i * (AXIS_WIDTH / 52) + LEFT_MARGIN;
                let idx: usize = i.try_into().unwrap();
                let tmp: i32; // get ready to hold the hi_temp to display
                let hi_result = rows[idx-1].try_get("tmax");
                match hi_result {
                    Ok(_) => { tmp = hi_result.unwrap(); } //set tmp to hi_temp
                    Err(_) => { continue; }
                }
                let y: f64 = f64::from(tmp) * pixel_per_degree; //calc how tall this line should be
                if z_line_offset <= 0.0 { // negative offsets are temps above 0 degrees F
                    y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;                   
                } else {
                    y_adj = (y + z_line_offset).round() as i32;
                }
                //println!("BOTTOM_LINE: {BOTTOM_LINE_Y}  Bar length: {y}  zero line: {z_line_offset}  y_adj: {y_adj}");
                dwg.draw(&Rectangle::new(
                    [(x, BOTTOM_LINE_Y - 2), (x+8, BOTTOM_LINE_Y - y_adj)],
                    Into::<ShapeStyle>::into(&RED).filled(),
                ))?
            }   
        },
        "Fort" => {    
            for i in 1..27 {
                let x = i * (AXIS_WIDTH / 26) + LEFT_MARGIN - 16;//-16 is a fundge factor to position bars correctly
                let idx: usize = i.try_into().unwrap();
                let tmp: i32;
                let hi_result = rows[idx-1].try_get("tmax");
                match hi_result {
                    Ok(_) => { tmp = hi_result.unwrap(); }
                    Err(_) => { continue; }
                }
                let y: f64 = f64::from(tmp) * pixel_per_degree;
                if z_line_offset <= 0.0 { // negative offsets are temps above 0 degrees F
                    y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;                   
                } else {
                    y_adj = (y + z_line_offset).round() as i32;
                }
                //let y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;
                //println!("BOTTOM_LINE: {BOTTOM_LINE_Y}  Bar length: {y}  zero line: {z_line_offset}  y_adj: {y_adj}");
                dwg.draw(&Rectangle::new(
                    [(x, BOTTOM_LINE_Y - 2), (x+18, BOTTOM_LINE_Y - y_adj)],
                    Into::<ShapeStyle>::into(&RED).filled(),
                ))?
            }   
        },
        "Month" => {
            for i in 1..13 {
                let x = i * (AXIS_WIDTH / 12) + LEFT_MARGIN - 50; //-50 is a fundge factor to position bars correctly
                let idx: usize = i.try_into().unwrap();
                let tmp: i32;
                let hi_result = rows[idx-1].try_get("tmax");
                match hi_result {
                    Ok(_) => { tmp = hi_result.unwrap(); }
                    Err(_) => { continue; }
                }
                let y: f64 = f64::from(tmp) * pixel_per_degree;
                if z_line_offset <= 0.0 { // negative offsets are temps above 0 degrees F
                    y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;                   
                } else {
                    y_adj = (y + z_line_offset).round() as i32;
                }
                //let y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;
                //println!("BOTTOM_LINE: {BOTTOM_LINE_Y}  Bar length: {y}  zero line: {z_line_offset}  y_adj: {y_adj}");
                dwg.draw(&Rectangle::new(
                    [(x, BOTTOM_LINE_Y - 2), (x+30, BOTTOM_LINE_Y - y_adj)], //2nd y, bigger number = shorter bars
                    Into::<ShapeStyle>::into(&RED).filled(),
                ))?;
            }
        },
        _ => println!("Unknown Period"),
    }
    Ok(())
}

fn draw_low_temps(dwg: &DrawingArea<BitMapBackend, Shift>, period: &str, z_line_offset: f64, pixel_per_degree: f64, rows: &Vec<MySqlRow>) -> Result<(), Box<dyn std::error::Error>>  {
    let mut y_adj: i32;
    match period {
        "Week" => {
            for i in 1..53 {
                let x = i * (AXIS_WIDTH / 52) +  LEFT_MARGIN;
                let idx: usize = i.try_into().unwrap();
                //let tmp: i32 = rows[idx-1].get("tmin");
                let tmp: i32;
                let low_result = rows[idx-1].try_get("tmin");
                match low_result {
                    Ok(_) =>  { tmp = low_result.unwrap(); }
                    Err(_) => { continue; }
                }
                let y: f64 = f64::from(tmp) * pixel_per_degree;
                if z_line_offset <= 0.0 { // negative offsets are temps above 0 degrees F
                    y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;                   
                } else {
                    y_adj = (y + z_line_offset).round() as i32;
                }
                //println!("bottom line: {BOTTOM_LINE_Y}  Bar length: {y}  zero line: {z_line_offset}  y_adj: {y_adj}");
                dwg.draw(&Rectangle::new(
                    [(x, BOTTOM_LINE_Y - 2), (x+8, BOTTOM_LINE_Y - y_adj)],
                    Into::<ShapeStyle>::into(&GREEN).filled(),
                ))?
            }
        },
        "Fort" => {
            for i in 1..27 {
                let x = i * (AXIS_WIDTH / 26) +  LEFT_MARGIN - 16;
                let idx: usize = i.try_into().unwrap();
                // let tmp: i32 = rows[idx-1].get("tmin");
                let tmp: i32;
                let low_result = rows[idx-1].try_get("tmin");
                match low_result {
                    Ok(_) =>  { tmp = low_result.unwrap(); }
                    Err(_) => { continue; }
                }
                let y: f64 = f64::from(tmp) * pixel_per_degree;
                if z_line_offset <= 0.0 { // negative offsets are temps above 0 degrees F
                    y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;                   
                } else {
                    y_adj = (y + z_line_offset).round() as i32;
                }
                //println!("bottom line: {BOTTOM_LINE_Y}  Bar length: {y}  zero line: {z_line_offset}  y_adj: {y_adj}");
                dwg.draw(&Rectangle::new(
                    [(x, BOTTOM_LINE_Y - 2), (x+18, BOTTOM_LINE_Y - y_adj)], //2nd y, bigger number = shorter bars
                    Into::<ShapeStyle>::into(&GREEN).filled(),
                ))?
            }
        },
        "Month" => {
            for i in 1..13 {
                let x = i * (AXIS_WIDTH / 12) + LEFT_MARGIN - 50;
                let idx: usize = i.try_into().unwrap();
                let tmp: i32;
                let low_result = rows[idx-1].try_get("tmin");
                match low_result {
                    Ok(_) =>  { tmp = low_result.unwrap(); }
                    Err(_) => { continue; }
                }
                let y: f64 = f64::from(tmp) * pixel_per_degree;
                if z_line_offset <= 0.0 { // negative offsets are temps above 0 degrees F
                    y_adj = ((y + z_line_offset) + pixel_per_degree).round() as i32;                   
                } else {
                    y_adj = (y + z_line_offset).round() as i32;
                }
                //println!("bottom line: {BOTTOM_LINE_Y}  Bar length: {y}  zero line: {z_line_offset}  y_adj: {y_adj}");
                dwg.draw(&Rectangle::new(
                    [(x, BOTTOM_LINE_Y - 2), (x+30, BOTTOM_LINE_Y - y_adj)], //2nd y, bigger number = shorter bars
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
        let y = TOP_MARGIN + i * V_TICK_HEIGHT;
        dwg.draw(&PathElement::new(
            vec![(LEFT_MARGIN+2, y), (AXIS_WIDTH + LEFT_MARGIN, y)],
            Into::<ShapeStyle>::into(RGBColor(128, 128, 128)).stroke_width(1),
        ))?;
        dwg.draw(&PathElement::new(  //draw tick mark on y axis
            vec![(LEFT_MARGIN -10, y), (LEFT_MARGIN , y)],
            Into::<ShapeStyle>::into(&BLACK).stroke_width(3),
        ))?;
        if i <= 9 {
            let v_tick_4 = V_TICK_HEIGHT / 4;
            let y_minor1 = y + v_tick_4;
            let y_minor2 = y + (v_tick_4 * 2);
            let y_minor3 = y + (v_tick_4 * 3);
            dwg.draw(&PathElement::new(
                vec![(LEFT_MARGIN+2, y_minor1), (AXIS_WIDTH + LEFT_MARGIN, y_minor1)],
                Into::<ShapeStyle>::into(RGBAColor(128, 128, 128, 0.5)).stroke_width(1),
            ))?;
            dwg.draw(&PathElement::new(
                vec![(LEFT_MARGIN+2, y_minor2), (AXIS_WIDTH + LEFT_MARGIN, y_minor2)],
                Into::<ShapeStyle>::into(RGBAColor(128, 128, 128, 0.5)).stroke_width(1),
            ))?;
            dwg.draw(&PathElement::new(
                vec![(LEFT_MARGIN+2, y_minor3), (AXIS_WIDTH + LEFT_MARGIN, y_minor3)],
                Into::<ShapeStyle>::into(RGBAColor(128, 128, 128, 0.5)).stroke_width(1),
            ))?;
        }
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
                         _y_lowest: i32,
                         y_highest: i32,
                         y_range: i32) -> Result<(), Box<dyn std::error::Error>> {
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
                let x = i * (AXIS_WIDTH / 26) + LEFT_MARGIN - 15;
                let i_str = i.to_string();
                dwg.draw_text(&i_str, &x_axis_style, (x - 1, AXIS_HEIGHT + TOP_MARGIN + (x_label_height / 2) as i32 + 1))?;
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
    let (y_label_width, y_label_height) = dwg.estimate_text_size(&format!("{}", y_highest), &y_axis_style)?;
    let temp: f64 = y_range as f64 / 10.0;
    //let tenth_range: i32 = temp.round() as i32; // amount to adjust for each horizontal grid line
    let tenth_range = temp; // amount to adjust for each horizontal grid line
    for i in 0..10 {
        let y = f64::from(TOP_MARGIN) + (f64::from(i) * f64::from(AXIS_HEIGHT)) / 10.0;
        let i_str = format!("{:.1}", (f64::from(y_highest) - (tenth_range * f64::from(i))));
        dwg.draw_text(&i_str, &y_axis_style, (LEFT_MARGIN - 24 - y_label_width as i32, y.round() as i32 - (y_label_height/2) as i32))?;
    }
    Ok(())
}
async fn get_city_min_max(pool: &Pool<MySql>, city: &str) -> Result<(i32, i32), sqlx::Error> {
    let query_string = format!("SELECT min_temp, max_temp FROM city_names WHERE name_of_city = '{}'", city); // Adjust table name as needed
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_string)
        .fetch_all(pool)
        .await?; // had to make this function return a Result to use the ? operator

    let lo: i32 = rows[0].get(0);
    let hi: i32 = rows[0].get(1);

    Ok((lo, hi))
}

async fn get_temps(pool: &Pool<MySql>, tperiod: &str, city: &str, year: i32) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_string = format!("SELECT tyear, {}, tmax, tmin FROM {} WHERE tyear = {}", tperiod, city, year ); // Adjust table name as needed
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_string)
        .fetch_all(pool)
        .await?; // had to make this function return a Result to use the ? operator
    Ok(rows)
}

fn print_avgs(tperiod: &str, city: &str, year: i32, rows: &Vec<MySqlRow>) {
    if rows.is_empty() {
        println!("No {} data found for {} in {}", tperiod, city, year);
        return;
    }
    println!("Avg {} temps for {} in {}", tperiod, city, year);
    let mut hi_temp: i32;
    let mut lo_temp: i32;
    for row in rows {
        let year: i32 = row.get("tyear");
        let week: i32 = row.get(1); // use index instead of tmonth/tfort/tweek
        let hi_result = row.try_get("tmax");
        match hi_result {
            Ok(_) => { hi_temp = hi_result.unwrap(); }
            Err(_) => { hi_temp = 999; }
        }
        let low_result = row.try_get("tmin");
        match low_result {
            Ok(_) =>  { lo_temp = low_result.unwrap(); }
            Err(_) => { lo_temp = -999; }
        }
        println!("{}-{}: Avg Hi={}, Avg Lo={}", year, week, hi_temp, lo_temp);
    }
}
async fn list_cities(pool: &Pool<MySql>) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_string = format!("SELECT name_of_city FROM city_names"); 
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_string)
        .fetch_all(pool)
        .await?; 
    Ok(rows)
}
async fn drop_city_sub_tables(pool: &Pool<MySql>, city: &str) -> Result<(), sqlx::Error>{
    let city_sub_month = format!("{city}_month");
    let city_sub_fort = format!("{city}_fort"); 
    let city_sub_week = format!("{city}_week"); 

    let drop_stmt = format!("DROP TABLE IF EXISTS {city_sub_month},{city_sub_fort},{city_sub_week};");

    let _result = sqlx::query(&drop_stmt).execute(pool).await?;

    Ok(())
}
async fn create_city_sub_tables(pool: &Pool<MySql>, city: &str) -> Result<(), sqlx::Error> {
    let city_sub_month = format!("{city}_month");
    let city_sub_fort = format!("{city}_fort"); 
    let city_sub_week = format!("{city}_week"); 

    let create_month_stmt = format!(r#"CREATE TABLE if NOT exists `{}` (
  `id` int(11) NOT NULL,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tmonth` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci ;"#, city_sub_month);
    let _result = sqlx::query(&create_month_stmt).execute(pool).await?;

    let create_week_stmt = format!(r#"CREATE TABLE if NOT exists `{}` (
  `id` int(11) NOT NULL,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tweek` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;"#, city_sub_week);
    let _result2 = sqlx::query(&create_week_stmt).execute(pool).await?;

    let create_fort_stmt = format!(r#"CREATE TABLE if NOT EXISTS `{}` (
  `id` int(11) NOT NULL,
  `station` char(12) DEFAULT NULL,
  `tyear` smallint(6) NOT NULL,
  `tfort` smallint(6) NOT NULL,
  `tmax` smallint(6) DEFAULT NULL,
  `tmin` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;"#, city_sub_fort);
    let _result3 = sqlx::query(&create_fort_stmt).execute(pool).await?;

    Ok(())
}

async fn get_first_year(pool: &Pool<MySql>,  city: &str) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate asc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(pool)
        .await?; 
    //println!("Number of First Year Rows found: {}", rows.len());
    Ok(rows)
}

async fn get_last_year(pool: &Pool<MySql>,  city: &str) -> Result<Vec<MySqlRow>, sqlx::Error> {
    let query_stmt_string = format!("SELECT tdate FROM {city} order by tdate desc limit 1");
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(&query_stmt_string)
        .fetch_all(pool)
        .await?; // had to make this function return a Result to use the ? operator
    //println!("Number of Last Year Rows found: {}", rows.len());
    Ok(rows)
}
