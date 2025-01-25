from datetime import datetime
from unittest import TestCase

from airbyte_cdk.sources.streams.concurrent.clamping import (
    DayClampingStrategy,
    MonthClampingStrategy,
    WeekClampingStrategy,
    Weekday,
)

_DATETIME_ON_TUESDAY = datetime(2025, 1, 14)
_DATETIME_ON_WEDNESDAY = datetime(2025, 1, 15)


class DayClampingStrategyTest(TestCase):
    def setUp(self) -> None:
        self._strategy = DayClampingStrategy()

    def test_when_clamp_then_remove_every_unit_smaller_than_days(self) -> None:
        result = self._strategy.clamp(datetime(2024, 1, 1, 20, 23, 3, 2039))
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0

    def test_given_last_day_of_month_when_clamp_then_result_is_next_month(self) -> None:
        result = self._strategy.clamp(datetime(2024, 1, 31))
        assert result == datetime(2024, 2, 1)

    def test_given_is_not_ceiling_when_clamp_then_just_remove_unit_smaller_than_days(self) -> None:
        strategy = DayClampingStrategy(is_ceiling=False)
        result = strategy.clamp(datetime(2024, 1, 1, 20, 23, 3, 1029))
        assert result == datetime(2024, 1, 1)


class MonthClampingStrategyTest(TestCase):
    def setUp(self) -> None:
        self._strategy = MonthClampingStrategy()

    def test_when_clamp_then_remove_every_unit_smaller_than_days(self) -> None:
        result = self._strategy.clamp(datetime(2024, 1, 1, 20, 23, 3, 2039))
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0

    def test_given_first_day_of_month_when_clamp_then_return_same_date(self) -> None:
        first_day_of_the_month = datetime(2024, 1, 1)
        result = self._strategy.clamp(first_day_of_the_month)
        assert result == first_day_of_the_month

    def test_given_day_of_month_is_not_1_when_clamp_then_return_first_day_of_next_month(
        self,
    ) -> None:
        result = self._strategy.clamp(datetime(2024, 1, 2))
        assert result == datetime(2024, 2, 1)

    def test_given_not_ceiling_and_day_of_month_is_not_1_when_clamp_then_return_first_day_of_next_month(
        self,
    ) -> None:
        strategy = MonthClampingStrategy(is_ceiling=False)
        result = strategy.clamp(datetime(2024, 1, 2))
        assert result == datetime(2024, 1, 1)


class WeekClampingStrategyTest(TestCase):
    def setUp(self) -> None:
        self._strategy = WeekClampingStrategy(Weekday.TUESDAY)

    def test_when_clamp_then_remove_every_unit_smaller_than_days(self) -> None:
        result = self._strategy.clamp(datetime(2024, 1, 1, 20, 23, 3, 2039))
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0

    def test_given_same_weekday_when_clamp_then_return_same_date(self) -> None:
        strategy = WeekClampingStrategy(Weekday.TUESDAY)
        result = strategy.clamp(_DATETIME_ON_TUESDAY)
        assert result == _DATETIME_ON_TUESDAY

    def test_given_not_weekday_before_target_when_clamp_then_return_next_occurrence_of_same_weekday(
        self,
    ) -> None:
        strategy = WeekClampingStrategy(Weekday.TUESDAY)
        result = strategy.clamp(_DATETIME_ON_WEDNESDAY)
        assert result == datetime(
            _DATETIME_ON_WEDNESDAY.year,
            _DATETIME_ON_WEDNESDAY.month,
            _DATETIME_ON_WEDNESDAY.day + 6,
        )

    def test_given_not_weekday_after_target_when_clamp_then_return_next_occurrence_of_same_weekday(
        self,
    ) -> None:
        strategy = WeekClampingStrategy(Weekday.FRIDAY)
        result = strategy.clamp(_DATETIME_ON_WEDNESDAY)
        assert result == datetime(
            _DATETIME_ON_WEDNESDAY.year,
            _DATETIME_ON_WEDNESDAY.month,
            _DATETIME_ON_WEDNESDAY.day + 2,
        )

    def test_given_not_ceiling_when_clamp_then_round_down(self) -> None:
        strategy = WeekClampingStrategy(Weekday.FRIDAY, is_ceiling=False)
        result = strategy.clamp(_DATETIME_ON_WEDNESDAY)
        assert result == datetime(
            _DATETIME_ON_WEDNESDAY.year,
            _DATETIME_ON_WEDNESDAY.month,
            _DATETIME_ON_WEDNESDAY.day - 5,
        )
