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


def get_earnings_stats(rider, start_time, end_time, calculate_total_pay=True):
    get_earnings_stats_sql = ("""select SUM(total_time) as total_time ,
                                SUM(total_pause_time) as total_pause_time,
                                SUM(total_problem_time)  as total_problem_time,
                                SUM(total_shift_hours)  as total_shift_hours,
                                SUM(total_over_time)  as total_over_time,
                                SUM(over_time_pay)  as over_time_pay ,
                                SUM(pay)  as total_pay
                                from rider_earnings_stats res inner join rider_shift rs on  res.rider_shift_id = rs.id 
                                where    rs.rider_id = '{}' AND rs.started_at BETWEEN '{}'
                                AND '{}'

                                """ .format(rider, start_time, end_time))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_earnings_stats_sql)
    get_stats = cursor.fetchall()

    total_pause_time = get_stats[0][1] or 0
    total_worked_time = get_stats[0][0] or 0
    total_problem_time = get_stats[0][2] or 0
    total_shift_hours = get_stats[0][3]
    total_active_hours = total_worked_time - total_pause_time - total_problem_time
    hours = round(total_active_hours, 2)
    hours_percent = round(total_active_hours * 100 / total_shift_hours, 1) if total_shift_hours else 0
    hours_percent = hours_percent if hours_percent <= 100 else 100  # added to round off to 100

    response = {'hours': hours, 'hours_percent': hours_percent}
    if calculate_total_pay:
        response['total_pay'] = get_stats[0][6] or 0
        response['over_time_pay'] = get_stats[0][5] or 0
        response['total_over_time'] = get_stats[0][4] or 0

    return response


def get_rider_order_stats(rider, start_time, end_time):
    get_rider_order_stats_sql = (""" select COUNT(os.id) as total_orders,
                                COUNT(  os.picked_up_at )  as total_picked_up_orders,
                                COUNT(  os.delivered_at )  as delivered_orders
                                from order_state os 
                                inner join `order` o on os.order_id = o.id 
                                WHERE (os.assigned_at BETWEEN '{}' AND '{}'
                                AND o.status in ("Delivered", "Cancelled", "Failed", "Invalid") AND os.rider_id='{}' )"""
                                 .format(start_time, end_time, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_order_stats_sql)
    get_rider_stats = cursor.fetchall()
    total_orders = get_rider_stats[0][0] or 0
    total_picked_up_orders = get_rider_stats[0][1] or 0
    delivered_orders = get_rider_stats[0][1] or 0
    total_failed_orders = total_picked_up_orders - delivered_orders
    return {
        'total_orders': total_orders,
        'total_picked_up_orders': total_picked_up_orders,
        'total_delivered_orders': delivered_orders,
        'total_failed_orders': total_failed_orders,
        'failed_rate': (round(total_failed_orders * 100 / total_picked_up_orders, 1)
                        if total_picked_up_orders else 0),
    }


def instance():
    instance_sql = ("""SELECT hourly_pay_min_app_on_time from city_configuration cc inner join rider r on
                        r.city_id = cc.id """)

    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(instance_sql)
    rider_instance = cursor.fetchall()
    return rider_instance


def can_get_minimum_guarantee(rider, start_time, end_time, total_pay):

    shifts_stats = get_earnings_stats(rider, start_time, end_time)
    rider_instance = instance()
    order_stats = get_rider_order_stats(rider, start_time, end_time)
    stats = {'shifts_stats': shifts_stats, 'order_stats': order_stats}

    return (total_pay < shifts_stats['total_pay'] and shifts_stats['hours_percent'] >=
         rider_instance[0][0]), stats


def get_rider_order_dates_stats(rider,week):
    get_rider_order_dates_stats_sql = ("""SELECT COUNT(id) as total_orders,
                                        Count(Case when os.picked_up_at is not null then os.id end) as total_picked_up_orders,
                                        Count(Case when os.delivered_at is not null then os.id end) as delivered_orders
                                        from order_state os where (DATE(CONVERT_TZ ( os.assigned_at ,UTC_DATE  , UTC_DATE )) IN {} AND os.rider_id = '{}')
                                        
                                        
                                        """.format(week, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_order_dates_stats_sql)
    get_rider_order = cursor.fetchall()
    total_orders = get_rider_order[0][0] or 0
    total_picked_up_orders = get_rider_order[0][1] or 0
    delivered_orders = get_rider_order[0][2] or 0
    total_failed_orders = total_picked_up_orders - delivered_orders
    return {
        'total_orders': total_orders,
        'total_picked_up_orders': total_picked_up_orders,
        'total_delivered_orders': delivered_orders,
        'total_failed_orders': total_failed_orders,
        'failed_rate': (round(total_failed_orders * 100 / total_picked_up_orders, 1)
                        if total_picked_up_orders else 0),
    }


def get_rider_order_accept_stats(rider, start_time, end_time):
    get_rider_order_accept_stats_sql = ("""select COUNT(rarl.id) as total_orders,
                                    COUNT(CASE when rarl.log_type="A" then rarl.id end) as accepted_orders,
                                    COUNT(CASE when rarl.log_type="R" then rarl.id end) as rejected_orders
                                    FROM rider_accept_reject_log rarl where rarl.created_at BETWEEN 
                                    '{}' AND '{}' AND rarl .rider_id ='{}' 
                                    """.format(start_time, end_time, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_order_accept_stats_sql)
    get_rider_order_accept = cursor.fetchall()
    total_orders = get_rider_order_accept[0][0] or 0
    accepted_orders = get_rider_order_accept[0][1] or 0
    rejected_orders = get_rider_order_accept[0][2] or 0
    return {
        'total_orders': total_orders,
        'total_accepted_orders': accepted_orders,
        'total_rejected_orders': rejected_orders,
        'acceptance_rate': round(accepted_orders * 100 / total_orders, 1) if total_orders else 0,
    }


def get_rider_on_time_delivery_stats(rider, start_time, end_time):
    get_rider_on_time_delivery_stats_sql = ("""select COUNT(o.id)  from order_state os inner join `order` o ON os.order_id = o.id 
                                            inner join algo_order_times aot on o.id =aot.order_id 
                                            WHERE (os.arrived_for_delivery_at <= (CASE WHEN aot.delivery_time_after_pickup IS NOT NULL THEN 
                                            aot.delivery_time_after_pickup 
                                            WHEN aot.delivery_time_after_pickup IS NULL THEN aot.delivery_time ELSE NULL END) AND 
                                            os.assigned_at BETWEEN '{}' AND '{}' AND os.delivered_at IS NOT NULL AND o.status = "Delivered" AND 
                                            os.rider_id = '{}')""".format(start_time, end_time, rider))

    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_on_time_delivery_stats_sql)
    get_rider_on_time = cursor.fetchall()
    return {
        'total_on_time_deliveries': get_rider_on_time[0][0]
    }


def get_rider_on_time_pickup_stats(rider, start_time, end_time):
    get_rider_on_time_pickup_stats_sql = ("""select COUNT(o.id)  from order_state os inner join `order` o ON os.order_id = o.id 
                                            inner join algo_order_times aot on o.id =aot.order_id 
                                            WHERE  (os.arrived_at <= (aot.rider_arrival_time) AND 
                                            os.assigned_at BETWEEN '{}' AND '{}' AND o.status IN 
                                            ("Delivered", "Cancelled", "Failed", "Invalid") AND os.picked_up_at IS NOT NULL AND os.rider_id = '{}')
                                            """.format(start_time, end_time, rider))

    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_rider_on_time_pickup_stats_sql)
    get_rider_on_time_pickup = cursor.fetchall()
    return {
        'total_on_time_pickups': get_rider_on_time_pickup[0][0]
    }


def get_on_time_rate(on_time_deliveries, on_time_pickups, total_delivered_orders, total_picked_up_orders):


    return int(round((on_time_deliveries + on_time_pickups) * 100 /
                     ((total_delivered_orders + total_picked_up_orders) or 1), 0))


def calculate_on_time_rates(rider, start_time, end_time,total_delivered_orders, total_picked_up_orders):
    on_time_delivery_stats = get_rider_on_time_delivery_stats(rider, start_time, end_time)
    total_on_time_deliveries = on_time_delivery_stats['total_on_time_deliveries']
    on_time_pickup_stats = get_rider_on_time_pickup_stats(rider, start_time, end_time)
    total_on_time_pickups = on_time_pickup_stats['total_on_time_pickups']

    return {"drop_off_rate": round(total_on_time_deliveries * 100 / (total_delivered_orders or 1)),
            "pickup_rate": round(total_on_time_pickups * 100 / (total_picked_up_orders or 1)),
            "on_time_rate": get_on_time_rate(total_on_time_deliveries, total_on_time_pickups, total_delivered_orders,
                                             total_picked_up_orders)}