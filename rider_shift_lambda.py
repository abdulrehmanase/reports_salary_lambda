import datetime

from utils import *
from sql import *
from decimal import Decimal
from datetime import timedelta
import json
from django.core.serializers.json import DjangoJSONEncoder

NIC = 'CNIC'
CITY = 'City'
RIDER_CATEGORY = "Rider Category"
RIDER_TYPE = "Rider Type"
TOTAL_PICKED_UP_ORDERS = 'Total Orders(Failed + Delivered)'
WEEKEND_ORDERS = 'Weekend Total Orders'
HOURS_WORKED = 'Sum of Hours worked(Worked hours - Pause time)'
UTR = 'UTR'
PICKUP_PAY = 'Pickup PKR (PP)'
DROP_OFF_DISTANCE_PAY = 'Total Drop-off Pay PKR (DDP)'
DROP_OFF_DISTANCE = "Drop-off Distance KM"
PICKUP_BONUS = 'Total Pickup bonus (PKR) (PB)'
PICKUP_DISTANCE = "Pickup Distance KM"
DROP_OFF_PAY = 'Total Drop-off pay (DP)'
# DELIVERY_CHARGES_BASED_PAY_PAY = RiderEarningsLogType.delivery_charges_based_pay.label
# PER_ORDER_PAY = RiderEarningsLogType.per_order_pay.label
# SLAB_BASED_PAY = RiderEarningsLogType.slab_based_pay.label
# LATE_NIGHT_BONUS = RiderEarningsLogType.late_night_bonus_pay.label
NO_SHOW_DAYS = 'No show days (only the days when at least one shift was scheduled)'
PENALTY = 'Penalty'
TOTAL_WITHOUT_GUARANTEE = 'Total without guarantee'
PER_HOUR_INCOME = 'Per hour income'
GUARANTEED_PAY_PER_HOUR = 'Per hour income on the basis of weighted average of hours'
ACCEPTANCE_RATE = 'Acceptance %'
FAILED_ORDERS = 'Failed orders'
UNACCEPTED_ORDERS = 'UnAccepted orders'
FAILED_RATE = 'Failed Order Percentage'
APP_ON_RATE = 'App on-time % (cumulative)'
GUARANTEE_QUALIFIES = 'Guarantee Qualifies'
GUARANTEE_RATE = 'Guarantee rate'
FOOD_ORDER_PAY = 'Food Order Pay'
HEALTH_CARE_ORDER_PAY = 'HealthCare Order Pay'
ERRAND_ORDER_PAY = "Errand Order Pay"
BOOKS_ORDER_PAY = 'Books Order Pay'
BEAUTY_ORDER_PAY = 'Beauty Order Pay'
BABY_CARE_ORDER_PAY = 'BabyCare Order Pay'
PANTRY_ORDER_PAY = 'Pantry Order Pay'
PHARMA_ORDER_PAY = 'Pharma Order Pay'
TIFFIN_ORDER_PAY = 'Tiffin Order Pay'
XOOM_ORDER_PAY = 'Xoom Order Pay'
TOTAL_PAY_WITH_GUARANTEE = 'Total pay with guarantee'
SIGN_UP_BONUS = 'Sign Up Bonus'
LAST_SALARY_DIFFERENCE = 'Last Salary Difference'
LOYALTY_BONUS_REDEEMED = 'Loyalty bonus redeemed'
OVER_TIME_PAY = 'Over Time Pay'
FINAL_PAYOUT = 'Final Payout'
FUEL_ALLOWANCE = "Fuel Allowance"
ON_TIME_RATE = 'One Time Rate'
TOTAL_ON_TIME_DELIVERIES = 'Number of on time drop offs'
TOTAL_ON_TIME_PICK_UPS = 'Number of on time pickups'
REFERRAL_BONUS = 'Referral Bonus'
CERTIFICATE_BONUS = 'Certificate Bonus'
SECURITY_DEPOSITS_DEDUCTION = "Security Deposits Deduction"
RIDER_WALLET = "Rider Wallet"


def rider_salary(start_date, end_date):
    data = get_dates(start_date, end_date)
    start_time, end_time = data['start_time'], data['end_time']
    start_date, end_date = data['start_date'], data['end_date']

    weekends = []
    for days in range((end_time - start_time).days + 1):
        date = start_date + timedelta(days=days)
        if 5 <= date.weekday() <= 6:
            weekends.append(str(date))
    weekendss = tuple(weekends)


    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_data(start_date, end_date))

    riders = cursor.fetchall()

    for rider in riders:
        rider_id = rider[0]
        pick_up_distance = get_rider_pickup_distances(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date, log_type="PB")
        drop_off_distance = get_rider_drop_off_distances(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date, log_type="DDP")
        rider_earning = get_rider_earnings(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date)
        pick_up_distance_bonus = rider_earning[0][0] or 0
        pick_up_pay = rider_earning[0][1] or 0
        drop_off_distance_pay = rider_earning[0][2] or 0
        drop_off_pay = rider_earning[0][3] or 0
        delivery_charges_based_pay = rider_earning[0][4] or 0
        per_order_pay = rider_earning[0][5] or 0
        slab_based_pay = rider_earning[0][6] or 0
        tips = rider_earning[0][7] or 0
        late_night_bonus = rider_earning[0][12] or 0

        rider_earning_category=get_rider_earnings_by_category(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date)

        food_order_pay = rider_earning_category[0][0] or 0
        healthcare_order_pay = rider_earning_category[0][1] or 0
        errand_pay = rider_earning_category[0][2] or 0
        books_order_pay = rider_earning_category[0][3] or 0
        beauty_order_pay = rider_earning_category[0][4] or 0
        babycare_order_pay = rider_earning_category[0][5] or 0
        pantry_order_pay = rider_earning_category[0][6] or 0
        pharma_order_pay = rider_earning_category[0][7] or 0
        tiffin_order_pay = rider_earning_category[0][8] or 0
        xoom_order_pay = rider_earning_category[0][9] or 0
        get_penalty = get_rider_penalty(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date)

        total_penalty, no_show_days = float(get_penalty['total_penalty']) , get_penalty['no_show_days']

        get_bouns = get_rider_bouns(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date)

        total_pay = max(pick_up_distance_bonus + pick_up_pay + drop_off_distance_pay + drop_off_pay +
                        delivery_charges_based_pay + per_order_pay + slab_based_pay + tips + late_night_bonus - total_penalty, 0)


        can_get_minimum_guarantees, stats = can_get_minimum_guarantee(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date , total_pay=total_pay)

        shifts_stats = stats["shifts_stats"]
        hours = shifts_stats['hours']
        app_on_rate = shifts_stats['hours_percent']
        guaranteed_pay = shifts_stats['total_pay']
        over_time_pay = shifts_stats['over_time_pay'] or 0
        total_picked_up_orders = orders_per_hour = acceptance_rate = per_hour_income = guaranteed_pay_per_hour = \
            total_failed_orders = failed_rate = guarantee_rate = weekend_orders = on_time_rate = \
            total_on_time_deliveries = total_on_time_pickups = total_unaccepted_orders = 0
        guarantee_qualifies = 'No'
        total_pay_with_guarantee = total_pay
        if hours:
            order_stats = stats['order_stats']
            total_picked_up_orders = order_stats['total_picked_up_orders']
            total_delivered_orders = order_stats['total_delivered_orders']
            total_failed_orders = order_stats['total_failed_orders']
            failed_rate = order_stats['failed_rate']
            orders_per_hour = round(total_delivered_orders / hours, 1)
            per_hour_income = round(Decimal(total_pay) / hours, 1)
            guaranteed_pay_per_hour = round(guaranteed_pay / hours, 2)
            weekend_orders_stats=get_rider_order_dates_stats(rider_id,weekendss)
            weekend_orders = weekend_orders_stats['total_picked_up_orders']
            get_rider_order=get_rider_order_accept_stats(rider=rider[0], start_time=start_date,
                                                                   end_time=end_date)
            acceptance_rate = get_rider_order['acceptance_rate']
            total_unaccepted_orders = get_rider_order['total_rejected_orders']




rider_salary("2021-5-10", "2021-10-10")

