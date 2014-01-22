import datetime

weekday_dict = {1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'}
month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}

start_time = datetime.datetime.now()

m_d_y = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)

mdy_list = str(m_d_y).split('-')

print weekday_dict[datetime.date.isoweekday(m_d_y)] + ' ' + month_dict[mdy_list[1]] + ' ' + mdy_list[2]