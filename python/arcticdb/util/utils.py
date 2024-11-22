from typing import Literal, Tuple, Union
import numpy as np
import pandas as pd

class  TimestampNumber:
    """
        Represents the timestamp as a typed number (can be seconds, minutes, hours),
        Thus allowing simple arithmetic with numbers - adding and subtracting
        specified number of same type units results in increasing or decreasing the timestamp with same
        amount of time that type/freq represents

        In other words any numbers added, subtracted or compared with this type
        are implicitly considered as instances of the same type seconds, minutes, hours
        and operations are carried on naturally.

        Supported are int operation for increment and decrement

        For comparison you can do it with any number or Timestamp object

        0 is Timestamp(0) etc
    """
    SupportedFreqTypes = Literal['s','m','h']
    
    DEFAULT_FREQ:SupportedFreqTypes = 's'

    TIME_ZERO : pd.Timestamp = pd.Timestamp(0)

    def __init__(self, value:np.int64, type:SupportedFreqTypes=DEFAULT_FREQ) -> None: 
        self.value:np.int64 = value
        self.__type:TimestampNumber.SupportedFreqTypes = type
    
    def get_type(self) -> SupportedFreqTypes:
        return self.__type
    
    def get_value(self) -> np.int64:
        """
            Returns the value as a number of specified units since Timestamp(0)
        """
        return self.value
    
    def to_timestamp(self) -> pd.Timestamp:
        result, *other = self.calculate_timestamp_after_n_periods(self.value, self.__type)
        return result
    
    def inc(self, add_number:np.int64) -> 'TimestampNumber':
        self.value = np.int64(self.value) + np.int64(add_number)
        return self

    def dec(self, add_number:np.int64) -> 'TimestampNumber':
        self.value = np.int64(self.value) - np.int64(add_number)
        return self
    
    def to_zero(self) -> 'TimestampNumber':
        self.value = 0
        return self
    
    @classmethod
    def calculate_timestamp_after_n_periods(cls, periods:int, freq:SupportedFreqTypes='s', 
            start_time: pd.Timestamp = TIME_ZERO) -> Union[pd.Timestamp, Tuple[pd.Timestamp, pd.Timestamp]]:
        """ 
            Calculates end timestamp, based on supplied start timestamp, by adding specified
            number of time periods denoted by 'freq' parameter ('s' - seconds, 'm' - minutes, 'h' - hours)
            If periods is negative the end timestamp will be prior to start timestamp

            returns first calculated timestamp and then sorted by time tuple of start time and end time
        """
        add=True
        if (periods < 0):
            periods:int = -periods
            add=False
        
        if (freq == 's'):
            if(add):
                end_time = start_time + pd.Timedelta(seconds=periods) 
            else:
                end_time = start_time - pd.Timedelta(seconds=periods) 
        elif  (freq == 'm'):     
            if(add):
                end_time = start_time + pd.Timedelta(minute=periods) 
            else:
                end_time = start_time - pd.Timedelta(minute=periods) 
        elif (freq == 'h'):        
            if(add):
                end_time = start_time + pd.Timedelta(hours=periods) 
            else:
                end_time = start_time - pd.Timedelta(hours=periods) 
        else:
            raise Exception("Not supported frequency")

        if (add):
            return end_time, (start_time, end_time)
        else:
            return end_time, (end_time , start_time)

    @classmethod
    def from_timestamp(cls, timestamp:pd.Timestamp, freq:SupportedFreqTypes=DEFAULT_FREQ) -> 'TimestampNumber':
        """
            Creates object from Timestamp, but will round the the internal 
            value to the floor of the specified type. For instance if time
            on the time stamp was 13:45:22 and specified freq is 'h' the resulting object 
            will be 13:00:00 if converted back to timestamp (larger time units will not be touched)

            In other words the resulting object will not be equal to
        """
        if (freq == 's'):
            return TimestampNumber(timestamp.value // 1000000000, 's')
        if (freq == 'm'):
            return TimestampNumber(timestamp.value // (1000000000*60), 'm')
        if (freq == 'h'):
            return TimestampNumber(timestamp.value // (1000000000*60*60), 'h')
        raise NotImplemented(f"Not supported param {freq}. Supported are {TimestampNumber.SupportedFreqTypes}")
    
    def __radd__(self, other):
        return self.value + other

    def __rsub__(self, other):
        return other - self.value  
    
    def __lt__(self, other) -> 'TimestampNumber':
        if (isinstance(other, pd.Timestamp)):
            return self.to_timestamp() < other
        if (isinstance(other, np.int64) or isinstance(other, np.uint64) or isinstance(other, int) or isinstance(other, float)) :
            return self.value < other
        else:
            raise NotImplemented("Only supports operations with integers and floats")
    
    def __eq__(self, other) -> 'TimestampNumber':
        if (isinstance(other, pd.Timestamp)):
            return self.to_timestamp() == other
        if (isinstance(other, np.int64) or isinstance(other, np.uint64) or isinstance(other, int) or isinstance(other, float)) :
            return self.value == other
        else:
            raise NotImplemented("Only supports operations with integers and floats")

    def __gt__(self, other) -> 'TimestampNumber':
        if (isinstance(other, pd.Timestamp)):
            return self.to_timestamp() > other
        if (isinstance(other, np.int64) or isinstance(other, np.uint64) or isinstance(other, int) or isinstance(other, float)) :
            return self.value > other
        else:
            raise NotImplemented("Only supports operations with integers and floats")

    def __add__(self, other) -> 'TimestampNumber':
        copy = TimestampNumber(self.value, self.__type)
        copy.inc(other)
        return copy
        
    def __sub__(self, other) -> 'TimestampNumber':
        copy = TimestampNumber(self.value, self.__type)
        copy.dec(other)
        return copy
    
    def __repr__(self):
        return f"TimestampTyped('{self.value} {self.__type}', '{str(self.to_timestamp())}')"

    def __str__(self):
        return str(self.to_timestamp())


