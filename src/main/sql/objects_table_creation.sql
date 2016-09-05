CREATE DATABASE IF NOT EXISTS spark_homework3;

USE spark_homework3;

DROP Table Objects_TEMPORARY;

CREATE TEMPORARY TABLE Objects_TEMPORARY (
  age DOUBLE,
  is_employed DOUBLE,
  is_retired DOUBLE,
  sex DOUBLE,
  number_of_children DOUBLE,
  number_of_dependent_people DOUBLE,
  education DOUBLE,
  marital_status DOUBLE,
  branch_of_employment DOUBLE,
  position_in_company DOUBLE,
  company_ownership DOUBLE,
  relationship_to_foreign_capital DOUBLE,
  direction_of_activity_inside_company DOUBLE,
  family_income DOUBLE,
  personal_income DOUBLE,
  city_of_registration DOUBLE,
  city_of_living DOUBLE,
  postal_address_of_city DOUBLE,
  city_of_branch_loan_was_taken DOUBLE,
  state DOUBLE,
  is_residence_mailing_address_match DOUBLE,
  is_registered_postal_address_match DOUBLE,
  is_postal_actual_address_match DOUBLE,
  portal_address DOUBLE,
  registration_area DOUBLE,
  owner_of_apartment DOUBLE,
  number_of_cars DOUBLE,
  is_owns_car_produced_domestically DOUBLE,
  is_owns_house_outside_city DOUBLE,
  is_owns_cottage DOUBLE,
  is_owns_garage DOUBLE,
  is_owns_land DOUBLE,
  amount_of_last_loan DOUBLE,
  term_of_loan DOUBLE,
  down_payment DOUBLE,
  is_driver_license_indivated DOUBLE,
  is_state_pension_fund_indivated DOUBLE,
  number_months_living_residence_address DOUBLE,
  duration_work_current_working_place DOUBLE,
  is_stationary_phone_at_residence_address DOUBLE,
  is_stationary_phone_at_registration_address DOUBLE,
  is_there_a_work_phone DOUBLE,
  number_of_loans DOUBLE,
  number_of_closed_loans DOUBLE,
  number_of_payments DOUBLE,
  number_payment_delays DOUBLE,
  max_order_number_delay DOUBLE,
  avg_amount_delayed_payment DOUBLE,
  max_amount_delayed_payment DOUBLE,
  utilized_cards DOUBLE
) row format delimited fields terminated by "," stored as textfile;

LOAD DATA INPATH '/user/maria_dev/Objects.csv' OVERWRITE INTO TABLE Objects_TEMPORARY;

DROP Table Objects;

create table Objects as SELECT *, ROW_NUMBER() OVER () AS row_num FROM Objects_TEMPORARY;