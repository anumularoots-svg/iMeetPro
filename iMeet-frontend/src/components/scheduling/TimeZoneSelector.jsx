import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  Autocomplete,
  Typography,
  Card,
  CardContent,
  Chip,
  Stack,
  Avatar,
  Grid,
  Paper,
  Button,
  Divider,
  Alert,
  IconButton
} from '@mui/material';
import {
  Public as PublicIcon,
  AccessTime as TimeIcon,
  LocationOn as LocationIcon,
  Schedule as ScheduleIcon,
  Star as StarIcon,
  StarBorder as StarBorderIcon,
  Refresh as RefreshIcon
} from '@mui/icons-material';

const TimeZoneSelector = ({ 
  selectedTimezone = 'Asia/Kolkata',
  onTimezoneChange,
  showPopular = true,
  showDetection = true,
  compact = false
}) => {
  const [timezone, setTimezone] = useState(selectedTimezone);
  const [detectedTimezone, setDetectedTimezone] = useState('');
  const [favorites, setFavorites] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');

  // Common timezones with regions
  const popularTimezones = [
    { value: 'Asia/Kolkata', label: 'India Standard Time (IST)', offset: '+05:30', region: 'Asia', flag: '🇮🇳' },
    { value: 'America/New_York', label: 'Eastern Time (ET)', offset: '-05:00', region: 'North America', flag: '🇺🇸' },
    { value: 'America/Los_Angeles', label: 'Pacific Time (PT)', offset: '-08:00', region: 'North America', flag: '🇺🇸' },
    { value: 'Europe/London', label: 'Greenwich Mean Time (GMT)', offset: '+00:00', region: 'Europe', flag: '🇬🇧' },
    { value: 'Europe/Paris', label: 'Central European Time (CET)', offset: '+01:00', region: 'Europe', flag: '🇫🇷' },
    { value: 'Asia/Tokyo', label: 'Japan Standard Time (JST)', offset: '+09:00', region: 'Asia', flag: '🇯🇵' },
    { value: 'Asia/Shanghai', label: 'China Standard Time (CST)', offset: '+08:00', region: 'Asia', flag: '🇨🇳' },
    { value: 'Australia/Sydney', label: 'Australian Eastern Time (AET)', offset: '+11:00', region: 'Oceania', flag: '🇦🇺' },
    { value: 'Asia/Dubai', label: 'Gulf Standard Time (GST)', offset: '+04:00', region: 'Asia', flag: '🇦🇪' },
    { value: 'America/Chicago', label: 'Central Time (CT)', offset: '-06:00', region: 'North America', flag: '🇺🇸' }
  ];

  // All world timezones
  const allTimezones = [
    // Africa
    { value: 'Africa/Cairo', label: 'Egypt Standard Time', offset: '+02:00', region: 'Africa', flag: '🇪🇬' },
    { value: 'Africa/Lagos', label: 'West Africa Time', offset: '+01:00', region: 'Africa', flag: '🇳🇬' },
    { value: 'Africa/Johannesburg', label: 'South Africa Standard Time', offset: '+02:00', region: 'Africa', flag: '🇿🇦' },
    
    // Asia
    { value: 'Asia/Kolkata', label: 'India Standard Time (IST)', offset: '+05:30', region: 'Asia', flag: '🇮🇳' },
    { value: 'Asia/Tokyo', label: 'Japan Standard Time (JST)', offset: '+09:00', region: 'Asia', flag: '🇯🇵' },
    { value: 'Asia/Shanghai', label: 'China Standard Time (CST)', offset: '+08:00', region: 'Asia', flag: '🇨🇳' },
    { value: 'Asia/Seoul', label: 'Korea Standard Time (KST)', offset: '+09:00', region: 'Asia', flag: '🇰🇷' },
    { value: 'Asia/Bangkok', label: 'Indochina Time', offset: '+07:00', region: 'Asia', flag: '🇹🇭' },
    { value: 'Asia/Singapore', label: 'Singapore Standard Time', offset: '+08:00', region: 'Asia', flag: '🇸🇬' },
    { value: 'Asia/Dubai', label: 'Gulf Standard Time (GST)', offset: '+04:00', region: 'Asia', flag: '🇦🇪' },
    { value: 'Asia/Riyadh', label: 'Arabia Standard Time', offset: '+03:00', region: 'Asia', flag: '🇸🇦' },
    { value: 'Asia/Karachi', label: 'Pakistan Standard Time', offset: '+05:00', region: 'Asia', flag: '🇵🇰' },
    { value: 'Asia/Dhaka', label: 'Bangladesh Standard Time', offset: '+06:00', region: 'Asia', flag: '🇧🇩' },
    
    // Europe
    { value: 'Europe/London', label: 'Greenwich Mean Time (GMT)', offset: '+00:00', region: 'Europe', flag: '🇬🇧' },
    { value: 'Europe/Paris', label: 'Central European Time (CET)', offset: '+01:00', region: 'Europe', flag: '🇫🇷' },
    { value: 'Europe/Berlin', label: 'Central European Time (CET)', offset: '+01:00', region: 'Europe', flag: '🇩🇪' },
    { value: 'Europe/Rome', label: 'Central European Time (CET)', offset: '+01:00', region: 'Europe', flag: '🇮🇹' },
    { value: 'Europe/Madrid', label: 'Central European Time (CET)', offset: '+01:00', region: 'Europe', flag: '🇪🇸' },
    { value: 'Europe/Amsterdam', label: 'Central European Time (CET)', offset: '+01:00', region: 'Europe', flag: '🇳🇱' },
    { value: 'Europe/Moscow', label: 'Moscow Standard Time (MSK)', offset: '+03:00', region: 'Europe', flag: '🇷🇺' },
    
    // North America
    { value: 'America/New_York', label: 'Eastern Time (ET)', offset: '-05:00', region: 'North America', flag: '🇺🇸' },
    { value: 'America/Chicago', label: 'Central Time (CT)', offset: '-06:00', region: 'North America', flag: '🇺🇸' },
    { value: 'America/Denver', label: 'Mountain Time (MT)', offset: '-07:00', region: 'North America', flag: '🇺🇸' },
    { value: 'America/Los_Angeles', label: 'Pacific Time (PT)', offset: '-08:00', region: 'North America', flag: '🇺🇸' },
    { value: 'America/Toronto', label: 'Eastern Time (Canada)', offset: '-05:00', region: 'North America', flag: '🇨🇦' },
    { value: 'America/Vancouver', label: 'Pacific Time (Canada)', offset: '-08:00', region: 'North America', flag: '🇨🇦' },
    { value: 'America/Mexico_City', label: 'Central Standard Time (Mexico)', offset: '-06:00', region: 'North America', flag: '🇲🇽' },
    
    // South America
    { value: 'America/Sao_Paulo', label: 'Brasilia Time', offset: '-03:00', region: 'South America', flag: '🇧🇷' },
    { value: 'America/Argentina/Buenos_Aires', label: 'Argentina Time', offset: '-03:00', region: 'South America', flag: '🇦🇷' },
    { value: 'America/Lima', label: 'Peru Time', offset: '-05:00', region: 'South America', flag: '🇵🇪' },
    { value: 'America/Santiago', label: 'Chile Standard Time', offset: '-04:00', region: 'South America', flag: '🇨🇱' },
    
    // Oceania
    { value: 'Australia/Sydney', label: 'Australian Eastern Time (AET)', offset: '+11:00', region: 'Oceania', flag: '🇦🇺' },
    { value: 'Australia/Melbourne', label: 'Australian Eastern Time (AET)', offset: '+11:00', region: 'Oceania', flag: '🇦🇺' },
    { value: 'Australia/Perth', label: 'Australian Western Time (AWT)', offset: '+08:00', region: 'Oceania', flag: '🇦🇺' },
    { value: 'Pacific/Auckland', label: 'New Zealand Standard Time (NZST)', offset: '+13:00', region: 'Oceania', flag: '🇳🇿' }
  ];

  useEffect(() => {
    // Detect user's timezone
    if (showDetection) {
      const detected = Intl.DateTimeFormat().resolvedOptions().timeZone;
      setDetectedTimezone(detected);
    }

    // Load favorites from localStorage
    const savedFavorites = localStorage.getItem('timezone-favorites');
    if (savedFavorites) {
      setFavorites(JSON.parse(savedFavorites));
    }
  }, [showDetection]);

  const getCurrentTime = (timezoneValue) => {
    try {
      return new Date().toLocaleTimeString('en-US', {
        timeZone: timezoneValue,
        hour12: true,
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch (error) {
      return 'Invalid';
    }
  };

  const handleTimezoneChange = (newTimezone) => {
    setTimezone(newTimezone);
    if (onTimezoneChange) {
      onTimezoneChange(newTimezone);
    }
  };

  const toggleFavorite = (timezoneValue) => {
    const newFavorites = favorites.includes(timezoneValue)
      ? favorites.filter(fav => fav !== timezoneValue)
      : [...favorites, timezoneValue];
    
    setFavorites(newFavorites);
    localStorage.setItem('timezone-favorites', JSON.stringify(newFavorites));
  };

  const useDetectedTimezone = () => {
    handleTimezoneChange(detectedTimezone);
  };

  const filteredTimezones = allTimezones.filter(tz =>
    tz.label.toLowerCase().includes(searchTerm.toLowerCase()) ||
    tz.value.toLowerCase().includes(searchTerm.toLowerCase()) ||
    tz.region.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const favoriteTimezones = allTimezones.filter(tz => favorites.includes(tz.value));

  if (compact) {
    return (
      <Autocomplete
        value={allTimezones.find(tz => tz.value === timezone) || null}
        onChange={(_, newValue) => newValue && handleTimezoneChange(newValue.value)}
        options={allTimezones}
        getOptionLabel={(option) => `${option.flag} ${option.label} (${option.offset})`}
        renderInput={(params) => (
          <TextField
            {...params}
            label="Timezone"
            variant="outlined"
            InputProps={{
              ...params.InputProps,
              startAdornment: <PublicIcon sx={{ mr: 1, color: 'text.secondary' }} />
            }}
          />
        )}
        renderOption={(props, option) => (
          <Box component="li" {...props}>
            <Avatar sx={{ mr: 2, bgcolor: 'transparent' }}>
              {option.flag}
            </Avatar>
            <Box>
              <Typography variant="body1">{option.label}</Typography>
              <Typography variant="caption" color="text.secondary">
                {option.offset} • {getCurrentTime(option.value)}
              </Typography>
            </Box>
          </Box>
        )}
      />
    );
  }

  return (
    <Card sx={{ borderRadius: 3, boxShadow: 3 }}>
      <CardContent sx={{ p: 4 }}>
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 4 }}>
          <Box sx={{ 
            p: 2, 
            borderRadius: 2, 
            bgcolor: 'primary.50', 
            display: 'flex', 
            alignItems: 'center',
            mr: 2 
          }}>
            <PublicIcon sx={{ color: 'primary.main', fontSize: 32 }} />
          </Box>
          <Box>
            <Typography variant="h4" color="primary.main" fontWeight="bold">
              Time Zone
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Select the timezone for your meeting
            </Typography>
          </Box>
        </Box>

        {/* Auto-detect Timezone */}
        {showDetection && detectedTimezone && (
          <Alert 
            severity="info" 
            sx={{ mb: 3, borderRadius: 2 }}
            action={
              <Button
                color="inherit"
                size="small"
                onClick={useDetectedTimezone}
                startIcon={<LocationIcon />}
              >
                Use This
              </Button>
            }
          >
            <Typography variant="body2">
              Detected timezone: <strong>{detectedTimezone}</strong> 
              ({getCurrentTime(detectedTimezone)})
            </Typography>
          </Alert>
        )}

        {/* Search */}
        <TextField
          fullWidth
          label="Search timezones"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          variant="outlined"
          sx={{ mb: 3 }}
          InputProps={{
            startAdornment: <TimeIcon sx={{ mr: 1, color: 'text.secondary' }} />
          }}
          placeholder="Search by city, country, or region..."
        />

        {/* Current Selection */}
        <Paper sx={{ p: 3, mb: 3, bgcolor: 'success.50', border: '1px solid', borderColor: 'success.200' }}>
          <Typography variant="h6" color="success.main" gutterBottom>
            Selected Timezone
          </Typography>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <Typography variant="h4" sx={{ mr: 2 }}>
                {allTimezones.find(tz => tz.value === timezone)?.flag || '🌍'}
              </Typography>
              <Box>
                <Typography variant="h6">
                  {allTimezones.find(tz => tz.value === timezone)?.label || timezone}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {allTimezones.find(tz => tz.value === timezone)?.offset} • Current time: {getCurrentTime(timezone)}
                </Typography>
              </Box>
            </Box>
            <IconButton
              onClick={() => toggleFavorite(timezone)}
              color={favorites.includes(timezone) ? 'warning' : 'default'}
            >
              {favorites.includes(timezone) ? <StarIcon /> : <StarBorderIcon />}
            </IconButton>
          </Box>
        </Paper>

        {/* Favorites */}
        {favoriteTimezones.length > 0 && (
          <Box sx={{ mb: 4 }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
              <StarIcon sx={{ mr: 1, color: 'warning.main' }} />
              Favorite Timezones
            </Typography>
            <Grid container spacing={2}>
              {favoriteTimezones.map((tz) => (
                <Grid item xs={12} sm={6} md={4} key={tz.value}>
                  <Card
                    onClick={() => handleTimezoneChange(tz.value)}
                    sx={{
                      cursor: 'pointer',
                      border: timezone === tz.value ? '2px solid' : '1px solid',
                      borderColor: timezone === tz.value ? 'primary.main' : 'grey.300',
                      bgcolor: timezone === tz.value ? 'primary.50' : 'background.paper',
                      '&:hover': {
                        transform: 'translateY(-2px)',
                        boxShadow: 2
                      },
                      transition: 'all 0.2s ease'
                    }}
                  >
                    <CardContent sx={{ p: 2 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                        <Typography variant="h6" sx={{ mr: 1 }}>
                          {tz.flag}
                        </Typography>
                        <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                          {tz.offset}
                        </Typography>
                      </Box>
                      <Typography variant="body2" noWrap>
                        {tz.label}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {getCurrentTime(tz.value)}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          </Box>
        )}

        {/* Popular Timezones */}
        {showPopular && !searchTerm && (
          <Box sx={{ mb: 4 }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
              <ScheduleIcon sx={{ mr: 1 }} />
              Popular Timezones
            </Typography>
            <Grid container spacing={2}>
              {popularTimezones.map((tz) => (
                <Grid item xs={12} sm={6} md={4} key={tz.value}>
                  <Card
                    onClick={() => handleTimezoneChange(tz.value)}
                    sx={{
                      cursor: 'pointer',
                      border: timezone === tz.value ? '2px solid' : '1px solid',
                      borderColor: timezone === tz.value ? 'primary.main' : 'grey.300',
                      bgcolor: timezone === tz.value ? 'primary.50' : 'background.paper',
                      '&:hover': {
                        transform: 'translateY(-2px)',
                        boxShadow: 2
                      },
                      transition: 'all 0.2s ease'
                    }}
                  >
                    <CardContent sx={{ p: 2, position: 'relative' }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                        <Typography variant="h6" sx={{ mr: 1 }}>
                          {tz.flag}
                        </Typography>
                        <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                          {tz.offset}
                        </Typography>
                        <IconButton
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            toggleFavorite(tz.value);
                          }}
                          sx={{ position: 'absolute', top: 4, right: 4 }}
                        >
                          {favorites.includes(tz.value) ? 
                            <StarIcon sx={{ fontSize: 16, color: 'warning.main' }} /> : 
                            <StarBorderIcon sx={{ fontSize: 16 }} />
                          }
                        </IconButton>
                      </Box>
                      <Typography variant="body2" noWrap sx={{ mb: 1 }}>
                        {tz.label}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {getCurrentTime(tz.value)}
                      </Typography>
                      <Chip
                        label={tz.region}
                        size="small"
                        sx={{ mt: 1, fontSize: '0.7rem' }}
                        color="secondary"
                        variant="outlined"
                      />
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          </Box>
        )}

        {/* All Timezones */}
        <Box>
          <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
            <PublicIcon sx={{ mr: 1 }} />
            {searchTerm ? `Search Results (${filteredTimezones.length})` : 'All Timezones'}
            {searchTerm && (
              <Button
                size="small"
                onClick={() => setSearchTerm('')}
                sx={{ ml: 2 }}
              >
                Clear
              </Button>
            )}
          </Typography>
          
          <Grid container spacing={2}>
            {(searchTerm ? filteredTimezones : allTimezones).map((tz) => (
              <Grid item xs={12} sm={6} md={4} key={tz.value}>
                <Card
                  onClick={() => handleTimezoneChange(tz.value)}
                  sx={{
                    cursor: 'pointer',
                    border: timezone === tz.value ? '2px solid' : '1px solid',
                    borderColor: timezone === tz.value ? 'primary.main' : 'grey.300',
                    bgcolor: timezone === tz.value ? 'primary.50' : 'background.paper',
                    '&:hover': {
                      transform: 'translateY(-2px)',
                      boxShadow: 2
                    },
                    transition: 'all 0.2s ease'
                  }}
                >
                  <CardContent sx={{ p: 2, position: 'relative' }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                      <Typography variant="h6" sx={{ mr: 1 }}>
                        {tz.flag}
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                        {tz.offset}
                      </Typography>
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleFavorite(tz.value);
                        }}
                        sx={{ position: 'absolute', top: 4, right: 4 }}
                      >
                        {favorites.includes(tz.value) ? 
                          <StarIcon sx={{ fontSize: 16, color: 'warning.main' }} /> : 
                          <StarBorderIcon sx={{ fontSize: 16 }} />
                        }
                      </IconButton>
                    </Box>
                    <Typography variant="body2" noWrap sx={{ mb: 1 }}>
                      {tz.label}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {getCurrentTime(tz.value)}
                    </Typography>
                    <Chip
                      label={tz.region}
                      size="small"
                      sx={{ mt: 1, fontSize: '0.7rem' }}
                      color="secondary"
                      variant="outlined"
                    />
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
          
          {filteredTimezones.length === 0 && searchTerm && (
            <Alert severity="warning" sx={{ mt: 2, borderRadius: 2 }}>
              No timezones found matching "{searchTerm}". Try searching for a city or country name.
            </Alert>
          )}
        </Box>
      </CardContent>
    </Card>
  );
};

export default TimeZoneSelector;