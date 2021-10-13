from utils import *
import pandas as pd
import pandasql as ps


def get_data(start_date, end_date):
    sql_query = ("""select rider.id , rider.job_model , rider.job_type , rider.nic , c.name ,rider.category,rider.cash_in_hand from rider INNER JOIN city c on rider.city_id = c.id where 
                    (rider.city_id is not NULL AND rider.id IN (SELECT rs.rider_id from rider_shift rs 
                    inner join shift s on rs.shift_id = s.id where s.start_at  BETWEEN  '{}' AND '{}' ) ) limit 5  """.format(start_date,end_date))
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

def loyalty_bonus_query(rider, start_time, end_time):
    loyalty_bonus_query_sql = ("""SELECT SUM(ROUND((ro.points) * (ro.value_per_point) )) as loyalty_bouns FROM redemption_request rr , redemption_option ro  
                                    WHERE (rr.rider_id = '{}' AND rr.status = "A" AND rr.updated_at 
                                    BETWEEN '{}' AND '{}')
                                    """.format(rider, start_time, end_time))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(loyalty_bonus_query_sql)
    get_rider_certificate = cursor.fetchall()
    return get_rider_certificate[0][0] or 0


def cash_in_hand_without_fuel_amounts():

    fuel_amount, order_ids = get_rider_non_paid_fuel_earnings()
    return fuel_amount



def get_rider_non_paid_fuel_earnings():
    pb_ddp_earning = ("""SELECT SUM(re.amount) as pick_up_and_drop_off_as_fuel_pay FROM rider_earnings re 
                        WHERE ((re.log_type = "PB" OR re.log_type = "DDP") AND 
                        re.is_fuel_paid = False AND re.rider_id = "3308")""")
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(pb_ddp_earning)
    get_earning = cursor.fetchall()
    fuel_amount = get_earning[0][0] or 0
    order_ids = order_ids_query()
    res_order_ids = []
    [res_order_ids.append(x) for x in order_ids if x not in res_order_ids]
    return fuel_amount, res_order_ids

def order_ids_query():
    order_ids_sql = ("""SELECT re.order_id FROM rider_earnings re 
                        WHERE ((re.log_type = "PB" OR re.log_type = "DDP") 
                        AND re.is_fuel_paid = False AND re.rider_id = "3308")""")
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(order_ids_sql)
    order_id = cursor.fetchall()
    return order_id


# def get_rider_certificate_earning():
#     already_paid = False
#     payment = 0
#     try:
#         df = get_rider_certificate_earnings()
#         previous_paid_stats = df[df['is_paid'] == 1]
#         if len(previous_paid_stats) > 0:
#             print("sdf")
#
#
#             payment = previous_paid_stats.iloc[0]['amount_to_pay']
#             already_paid = True
#         stats = df[df['is_paid'] == 0]
#
#         if len(stats) > 0:
#             for item in range(len(stats)):
#                   certificate_instance = stats.iloc[item]['id']
#                   c=stats.iloc[item]['created_at']
#                   time_diff = int((datetime.now() - c).total_seconds() / 3600)
#                   df = df[df['id'] !=certificate_instance ]
#
#
#     except:
#         pass


# def get_rider_certificate_earnings():
#
#     get_rider_certificate_earning_sql = ("""select c.id , rc.is_paid,rc.amount_to_pay,c.eligible_after_hours,c.created_at from rider_certificate rc  inner join certification c on rc.certificate_id =c.id
#                                             WHERE rc.created_at
#                                             BETWEEN "2021-05-10 00:00:00" AND "2021-10-10 00:00:00" OR rc.paid_at
#                                             BETWEEN "2021-05-10 00:00:00" AND "2021-10-10 00:00:00" AND rc.rider_id = "6045"
#                                              """)
#     connection = connect_to_db()
#     cursor = connection.cursor()
#     cursor.execute(get_rider_certificate_earning_sql)
#     get_rider_certificate = cursor.fetchall()
#
#     column_names = ('id', 'is_paid','amount_to_pay', 'eligible_after_hours','created_at')
#     # results = execute_store_procedure(procedure_name, params, sql=sql)
#     df = pd.DataFrame(get_rider_certificate, columns=column_names)
#     # x =df[df['is_paid']==1]
#     # print(df)


    # return df
# def logistics_instance():
#     logistics_instance_sql = ("""select lc.enforce_equipment_deposit_date ,lc.enable_equipment_cost_deduction,
#                                 lc.equipment_cost,lc.security_deposit from logistics_configuration lc """)
#     connection = connect_to_db()
#     cursor = connection.cursor()
#     cursor.execute(logistics_instance_sql)
#     log_instance = cursor.fetchall()
#     return log_instance

# def deduct_security_deposit(rider, start_date, end_date, salary):
#     config = logistics_instance()
#     enforce_date = config[0][0]
#     enable_equipment_deduction = bool(config[0][1])
#     equipment_cost = config[0][2]
#     security_deposits_cost = config[0][3]
#     rider_shift = rider_shift_query()
#     first_shift_date = rider_shift[0][0].date() if rider_shift else None
#     if not first_shift_date:  # No deduction for riders with no shift
#         return salary, 0
#     if enable_equipment_deduction:
#         equipment_deduction = rider_deposit()
#         equipment_deduction_date = equipment_deduction[0][0].date() if equipment_deduction else None
#         equipment_deduction_amount = 0
#         equipment_deduction_amount = 0
#         if end_date > enforce_date and salary > equipment_cost:  # check if we are calculating salary after enforce day
#             if first_shift_date > enforce_date:
#                 if equipment_deduction_date and start_date <= equipment_deduction_date <= end_date:  # Checks if security deposited deducted in the same date range
#                     salary -= equipment_cost
#                     equipment_deduction_amount = equipment_cost
#                 elif not equipment_deduction_date:  # if no deduction ever happened
#                     salary -= equipment_cost
#                     equipment_deduction_amount = equipment_cost
#                     RiderSecurityDeposit.objects.create(rider=rider, type=RiderSecurityDepositTypes.equipment.value,
#                                                         amount=equipment_cost)

# def rider_shift_query():
#     rider_shift_query_sql = ("""SELECT rs.created_at FROM rider_shift rs WHERE  (rs.rider_id = "3308"
#                             AND rs.started_at IS NOT NULL) ORDER BY rs.id ASC
#                             LIMIT 1""")
#     connection = connect_to_db()
#     cursor = connection.cursor()
#     cursor.execute(rider_shift_query_sql)
#     log_instance_data = cursor.fetchall()
#     return log_instance_data
#
# def rider_deposit():
#     rider_deposit_sql = ("""SELECT rsd.created_at FROM rider_security_deposit rsd WHERE
#                         rsd.rider_id = "3308" AND rsd.`type` = "e" ORDER BY rsd.id DESC
#                         limit 1""")
#     connection = connect_to_db()
#     cursor = connection.cursor()
#     cursor.execute(rider_deposit_sql)
#     log_rider_data = cursor.fetchall()
#     return log_rider_data