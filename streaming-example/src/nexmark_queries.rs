use datafusion::{
    prelude::{SessionContext, DataFrame}
};

pub async fn nex_mark_q5(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"
            WITH WindowedBids AS (
                SELECT auction, COUNT(*) AS num
                FROM Bid
                WHERE CAST(date_time AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '60 minutes'
                GROUP BY auction
            ),
            MaxBidCount AS (
                SELECT MAX(num) AS max_num
                FROM WindowedBids)
            SELECT auction
            FROM WindowedBids
            WHERE num = (SELECT max_num FROM MaxBidCount);
            "#,
        )
        .await?;

    Ok(df)
}

pub async fn nex_mark_q4(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"SELECT 
                    C.id AS category_id,
                    AVG(Q.final) AS avg_final_price
                FROM 
                    Category C
                JOIN 
                    (
                        SELECT 
                            MAX(B.price) AS final, 
                            A.category AS category_id
                        FROM 
                            Auction A
                        JOIN 
                            Bid B 
                        ON 
                            A.id = B.auction
                        WHERE 
                            B.date_time < A.expires 
                            AND CAST( A.expires AS TIMESTAMP) < CURRENT_TIMESTAMP
                        GROUP BY 
                            A.id, A.category
                    ) Q
                ON 
                    Q.category_id = C.id
                GROUP BY 
                    C.id;
            "#,
        )
        .await?;

    Ok(df)
}

pub async fn nex_mark_q3(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"SELECT P.name, P.city, P.state, A.id 
            FROM Auction A JOIN Person P ON A.seller = P.id 
            WHERE (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA') AND A.category = 10;
            "#,
        )
        .await?;

    Ok(df)
}

pub async fn nex_mark_q2(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"
            SELECT 
                auction, 
                price
            FROM 
                Bid
            WHERE 
                auction = 1007 
                OR auction = 1020 
                OR auction = 2001 
                OR auction = 2019 
                OR auction = 2087;
            "#,
        )
        .await?;

    Ok(df)
}

pub async fn nex_mark_q1(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"
            SELECT 
                auction, 
                price, 
                bidder, 
                date_time
            FROM 
                bid;
            "#,
        )
        .await?;

    Ok(df)
}