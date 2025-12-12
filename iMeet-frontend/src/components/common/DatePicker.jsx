import React, { useState } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Popover,
  Paper,
  Typography,
  Button,
  Stack,
  Grid,
  useTheme,
  alpha,
  Chip
} from '@mui/material';
import {
  CalendarToday as CalendarIcon,
  NavigateBefore as PrevIcon,
  NavigateNext as NextIcon,
  Today as TodayIcon
} from '@mui/icons-material';
import { format, startOfMonth, endOfMonth, eachDayOfInterval, 
         isSameMonth, isSameDay, addMonths, subMonths, isToday } from 'date-fns';

const DatePicker = ({
  value,
  onChange,
  label = "Select Date",
  placeholder = "Choose a date",
  minDate,
  maxDate,
  disabled = false,
  disabledDates = [],
  highlightedDates = [],
  size = "medium",
  fullWidth = true,
  error = false,
  helperText = "",
  showTodayButton = true,
  variant = "outlined"
}) => {
  const theme = useTheme();
  const [anchorEl, setAnchorEl] = useState(null);
  const [currentMonth, setCurrentMonth] = useState(value || new Date());
  
  const open = Boolean(anchorEl);

  const handleClick = (event) => {
    if (!disabled) {
      setAnchorEl(event.currentTarget);
    }
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleDateSelect = (date) => {
    onChange(date);
    handleClose();
  };

  const handleTodayClick = () => {
    const today = new Date();
    onChange(today);
    setCurrentMonth(today);
    handleClose();
  };

  const isDateDisabled = (date) => {
    if (minDate && date < minDate) return true;
    if (maxDate && date > maxDate) return true;
    return disabledDates.some(disabledDate => isSameDay(date, disabledDate));
  };

  const isDateHighlighted = (date) => {
    return highlightedDates.some(highlightedDate => isSameDay(date, highlightedDate));
  };

  const monthStart = startOfMonth(currentMonth);
  const monthEnd = endOfMonth(currentMonth);
  const daysInMonth = eachDayOfInterval({ start: monthStart, end: monthEnd });

  // Add padding days for the calendar grid
  const startDate = new Date(monthStart);
  startDate.setDate(startDate.getDate() - monthStart.getDay());
  
  const endDate = new Date(monthEnd);
  endDate.setDate(endDate.getDate() + (6 - monthEnd.getDay()));
  
  const calendarDays = eachDayOfInterval({ start: startDate, end: endDate });

  const weekDays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

  return (
    <Box>
      <TextField
        fullWidth={fullWidth}
        size={size}
        variant={variant}
        label={label}
        placeholder={placeholder}
        value={value ? format(value, 'MMM dd, yyyy') : ''}
        onClick={handleClick}
        disabled={disabled}
        error={error}
        helperText={helperText}
        InputProps={{
          readOnly: true,
          endAdornment: (
            <IconButton
              onClick={handleClick}
              disabled={disabled}
              sx={{ 
                color: theme.palette.text.secondary,
                '&:hover': {
                  backgroundColor: alpha(theme.palette.primary.main, 0.1),
                  color: theme.palette.primary.main
                }
              }}
            >
              <CalendarIcon />
            </IconButton>
          ),
          sx: {
            cursor: disabled ? 'default' : 'pointer',
            '&:hover': !disabled ? {
              backgroundColor: alpha(theme.palette.primary.main, 0.02),
            } : {},
          }
        }}
        sx={{
          '& .MuiOutlinedInput-root': {
            '&:hover fieldset': !disabled ? {
              borderColor: alpha(theme.palette.primary.main, 0.5),
            } : {},
          }
        }}
      />

      <Popover
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'left',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'left',
        }}
        PaperProps={{
          sx: {
            p: 2,
            borderRadius: 2,
            boxShadow: theme.shadows[8],
            border: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
            maxWidth: 320,
          }
        }}
      >
        <Paper elevation={0}>
          {/* Calendar Header */}
          <Stack direction="row" alignItems="center" justifyContent="space-between" mb={2}>
            <IconButton
              onClick={() => setCurrentMonth(subMonths(currentMonth, 1))}
              size="small"
              sx={{
                '&:hover': {
                  backgroundColor: alpha(theme.palette.primary.main, 0.1),
                  color: theme.palette.primary.main
                }
              }}
            >
              <PrevIcon />
            </IconButton>
            
            <Typography variant="h6" fontWeight={600}>
              {format(currentMonth, 'MMMM yyyy')}
            </Typography>
            
            <IconButton
              onClick={() => setCurrentMonth(addMonths(currentMonth, 1))}
              size="small"
              sx={{
                '&:hover': {
                  backgroundColor: alpha(theme.palette.primary.main, 0.1),
                  color: theme.palette.primary.main
                }
              }}
            >
              <NextIcon />
            </IconButton>
          </Stack>

          {/* Today Button */}
          {showTodayButton && (
            <Box mb={2} display="flex" justifyContent="center">
              <Chip
                icon={<TodayIcon />}
                label="Today"
                variant="outlined"
                size="small"
                onClick={handleTodayClick}
                sx={{
                  borderColor: theme.palette.primary.main,
                  color: theme.palette.primary.main,
                  '&:hover': {
                    backgroundColor: alpha(theme.palette.primary.main, 0.1),
                  }
                }}
              />
            </Box>
          )}

          {/* Week Days Header */}
          <Grid container spacing={0} mb={1}>
            {weekDays.map((day) => (
              <Grid item xs key={day}>
                <Box
                  display="flex"
                  alignItems="center"
                  justifyContent="center"
                  height={32}
                >
                  <Typography
                    variant="caption"
                    fontWeight={600}
                    color="text.secondary"
                  >
                    {day}
                  </Typography>
                </Box>
              </Grid>
            ))}
          </Grid>

          {/* Calendar Days */}
          <Grid container spacing={0}>
            {calendarDays.map((date, index) => {
              const isCurrentMonth = isSameMonth(date, currentMonth);
              const isSelected = value && isSameDay(date, value);
              const isDisabled = isDateDisabled(date);
              const isHighlighted = isDateHighlighted(date);
              const isTodayDate = isToday(date);

              return (
                <Grid item xs key={index}>
                  <Button
                    onClick={() => !isDisabled && handleDateSelect(date)}
                    disabled={isDisabled}
                    sx={{
                      width: 36,
                      height: 36,
                      minWidth: 'unset',
                      borderRadius: 2,
                      color: isCurrentMonth ? 'text.primary' : 'text.disabled',
                      backgroundColor: isSelected
                        ? theme.palette.primary.main
                        : isHighlighted
                        ? alpha(theme.palette.warning.main, 0.2)
                        : 'transparent',
                      border: isTodayDate && !isSelected
                        ? `2px solid ${theme.palette.primary.main}`
                        : '2px solid transparent',
                      '&:hover': !isDisabled ? {
                        backgroundColor: isSelected
                          ? theme.palette.primary.dark
                          : alpha(theme.palette.primary.main, 0.1),
                      } : {},
                      '&.Mui-disabled': {
                        color: theme.palette.text.disabled,
                        backgroundColor: 'transparent',
                      }
                    }}
                  >
                    <Typography
                      variant="body2"
                      fontWeight={isSelected || isTodayDate ? 600 : 400}
                      color={isSelected ? 'primary.contrastText' : 'inherit'}
                    >
                      {format(date, 'd')}
                    </Typography>
                  </Button>
                </Grid>
              );
            })}
          </Grid>

          {/* Footer Actions */}
          <Stack direction="row" justifyContent="flex-end" spacing={1} mt={2}>
            <Button
              size="small"
              onClick={handleClose}
              sx={{ color: 'text.secondary' }}
            >
              Cancel
            </Button>
          </Stack>
        </Paper>
      </Popover>
    </Box>
  );
};

export default DatePicker;