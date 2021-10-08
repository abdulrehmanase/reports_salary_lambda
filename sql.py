from utils import *


def get_data(start_date, end_date):
    sql_query = ("""select rider.id  from rider INNER JOIN city c on rider.city_id = c.id where 
                    (rider.city_id is not NULL AND rider.id IN (SELECT rs.rider_id from rider_shift rs 
                    inner join shift s on rs.shift_id = s.id where s.start_at  BETWEEN  '{}' AND '{}' ) )   """.format(start_date,end_date))
    return sql_query


def get_rider_pickup_distances(rider, start_time, end_time, log_type):
    pick_up_distance_sql = ("""select  sum(od.pickup_distance) from `order`o right join rider_earnings re 
                    on o.id=re.order_id
                    inner join order_distance od 
                    on od.order_id = o.id  
                    WHERE (re.created_at BETWEEN '{}' AND 
                    '{}') AND 
                    re.log_type ='{}' and
                    re.rider_id='{}'
                    """.format(start_time, end_time, log_type, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(pick_up_distance_sql)
    pick_up_distance = cursor.fetchall()
    if pick_up_distance[0][0]:
        return pick_up_distance[0][0]
    return 0


def get_rider_drop_off_distances(rider, start_time, end_time, log_type):

    drop_off_distance_sql = (""" select sum(od.delivered_distance) from `order`o right join rider_earnings re 
                                on o.id=re.order_id
                                inner join order_distance od 
                                on od.order_id = o.id  
                                WHERE (re.created_at BETWEEN '{}' AND 
                                '{}') AND 
                                re.log_type ='{}' and
                                re.rider_id='{}'
                                 """.format(start_time, end_time, log_type, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(drop_off_distance_sql)
    drop_off_distance = cursor.fetchall()
    if drop_off_distance[0][0]:
        return drop_off_distance[0][0]
    return 0


def get_rider_earnings(rider, start_time, end_time):

    rider_earnings_sql = ("""SELECT  
                        SUM(CASE when re.log_type="PB" then amount  END) as pick_up_distance_bonus ,
                        SUM(CASE when re.log_type="PP" then amount  END) as pick_up_pay ,
                        SUM(CASE when re.log_type="DDP" then amount  END) as drop_off_distance_pay,
                        SUM(CASE when re.log_type="DP" then amount  END) as drop_off_pay,
                        SUM(CASE when re.log_type="DCP" then amount  END) as delivery_charges_based_pay,
                        SUM(CASE when re.log_type="FP" then amount  END) as per_order_pay,
                        SUM(CASE when re.log_type="SBP" then amount  END) as slab_based_pay,
                        SUM(CASE when re.log_type="TP" then amount  END) as tip_pay,
                        
                        COUNT(case when re.log_type="PP" then order_id  END) total_pick_ups,
                        COUNT(case when re.log_type="DP" then order_id  END) total_drop_offs,
                        COUNT(case when re.log_type="FP" then order_id  END) total_per_order_pays,
                        COUNT(case when re.log_type="SBP" then order_id  END) total_slab_based_pays,
                        SUM(CASE when re.log_type="LNB" then amount  END) as total_late_night_bonus_pay
                        from rider_earnings re  where re.created_at BETWEEN '{}'
                        AND '{}'
                        AND re.rider_id ='{}' 
                        """.format(start_time, end_time, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(rider_earnings_sql)
    drop_off_distance = cursor.fetchall()
    return drop_off_distance


def get_rider_earnings_by_category(rider, start_time, end_time):
    get_rider_earnings_by_category_sql=("""select 
                                    SUM(CASE when o.category="food" then amount  END) as food,
                                    SUM(CASE when o.category="healthcare" then amount  END) as healthcare,
                                    SUM(CASE when o.category="errand" then amount END) as errand,
                                    SUM(CASE when o.category="books" then amount  END) as books,
                                    SUM(CASE when o.category="beauty" then amount  END) as beauty,
                                    SUM(CASE when o.category="babycare" then amount  END) as babycare,
                                    SUM(CASE when o.category="pantry" then amount  END) as pantry,
                                    SUM(CASE when o.category="pharma" then amount  END) as pharma,
                                    SUM(CASE when o.category="tiffin" then amount  END) as tiffin,
                                    SUM(CASE when o.category="xoom" then amount  END) as xoom
                                    from  rider_earnings re 
                                    right join `order` o on re.order_id =  o.id 
                                    WHERE  re.created_at BETWEEN '{}'
                                    AND '{}'
                                    AND re.rider_id ='{}' """.format(start_time,end_time,rider))

    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_earnings_by_category_sql)
    rider_earning_by_category = cursor.fetchall()
    return rider_earning_by_category


def get_rider_penalty(rider, start_time, end_time):
    get_rider_penalty_sql = ("""select SUM(rp.amount ) , COUNT(id) as total_penalty  
                                from rider_penalty rp WHERE rp.created_date BETWEEN  '{}' AND 
                                '{}' AND 
                                rp.rider_id ='{}'  """ .format(start_time, end_time, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_penalty_sql)
    get_penalty = cursor.fetchall()
    return {
        'total_penalty': get_penalty[0][0] or 0,
        'no_show_days': get_penalty[0][1] or 0,
    }


def get_rider_bouns(rider, start_time, end_time):
    get_rider_bouns_sql = ("""select SUM(bonus_amount) as total_bonus from rider_referral_bonus_log rrbl WHERE 
                                rrbl .created_at BETWEEN  '{}' AND 
                                '{}' AND 
                                rrbl.referred_by_id ='{}' """.format(start_time, end_time, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_bouns_sql)
    get_bouns = cursor.fetchall()
    return get_bouns[0][0] or 0