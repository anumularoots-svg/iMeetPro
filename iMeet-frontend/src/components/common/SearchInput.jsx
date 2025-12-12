import React, { useState, useCallback } from 'react';
import {
  TextField,
  InputAdornment,
  IconButton,
  Box,
  Chip,
  Stack,
  useTheme,
  alpha
} from '@mui/material';
import {
  Search as SearchIcon,
  Clear as ClearIcon,
  FilterList as FilterIcon
} from '@mui/icons-material';
import { debounce } from 'lodash';

const SearchInput = ({
  placeholder = "Search...",
  onSearch,
  onClear,
  filters = [],
  activeFilters = [],
  onFilterChange,
  showFilters = false,
  size = "medium",
  fullWidth = true,
  variant = "outlined",
  sx = {}
}) => {
  const theme = useTheme();
  const [searchValue, setSearchValue] = useState('');
  const [showFilterChips, setShowFilterChips] = useState(false);

  // Debounced search function
  const debouncedSearch = useCallback(
    debounce((value) => {
      if (onSearch) {
        onSearch(value);
      }
    }, 300),
    [onSearch]
  );

  const handleSearchChange = (event) => {
    const value = event.target.value;
    setSearchValue(value);
    debouncedSearch(value);
  };

  const handleClear = () => {
    setSearchValue('');
    if (onClear) {
      onClear();
    }
    if (onSearch) {
      onSearch('');
    }
  };

  const handleFilterClick = (filter) => {
    if (onFilterChange) {
      const newActiveFilters = activeFilters.includes(filter)
        ? activeFilters.filter(f => f !== filter)
        : [...activeFilters, filter];
      onFilterChange(newActiveFilters);
    }
  };

  const toggleFilters = () => {
    setShowFilterChips(!showFilterChips);
  };

  return (
    <Box sx={{ width: fullWidth ? '100%' : 'auto', ...sx }}>
      {/* Search Input */}
      <TextField
        fullWidth={fullWidth}
        size={size}
        variant={variant}
        placeholder={placeholder}
        value={searchValue}
        onChange={handleSearchChange}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <SearchIcon 
                sx={{ 
                  color: theme.palette.text.secondary,
                  fontSize: size === 'small' ? '1.2rem' : '1.5rem'
                }} 
              />
            </InputAdornment>
          ),
          endAdornment: (
            <InputAdornment position="end">
              <Stack direction="row" spacing={0.5}>
                {searchValue && (
                  <IconButton
                    size="small"
                    onClick={handleClear}
                    sx={{ 
                      color: theme.palette.text.secondary,
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.error.main, 0.1),
                        color: theme.palette.error.main
                      }
                    }}
                  >
                    <ClearIcon fontSize="small" />
                  </IconButton>
                )}
                {showFilters && (
                  <IconButton
                    size="small"
                    onClick={toggleFilters}
                    sx={{ 
                      color: showFilterChips ? theme.palette.primary.main : theme.palette.text.secondary,
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.1),
                        color: theme.palette.primary.main
                      }
                    }}
                  >
                    <FilterIcon fontSize="small" />
                  </IconButton>
                )}
              </Stack>
            </InputAdornment>
          ),
          sx: {
            borderRadius: 2,
            backgroundColor: alpha(theme.palette.background.paper, 0.8),
            '&:hover': {
              backgroundColor: theme.palette.background.paper,
            },
            '&.Mui-focused': {
              backgroundColor: theme.palette.background.paper,
              boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
            }
          }
        }}
        sx={{
          '& .MuiOutlinedInput-root': {
            '& fieldset': {
              borderColor: alpha(theme.palette.divider, 0.3),
            },
            '&:hover fieldset': {
              borderColor: alpha(theme.palette.primary.main, 0.5),
            },
            '&.Mui-focused fieldset': {
              borderColor: theme.palette.primary.main,
              borderWidth: 2,
            },
          },
        }}
      />

      {/* Filter Chips */}
      {showFilters && showFilterChips && filters.length > 0 && (
        <Box sx={{ mt: 1 }}>
          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
            {filters.map((filter) => (
              <Chip
                key={filter}
                label={filter}
                size="small"
                variant={activeFilters.includes(filter) ? "filled" : "outlined"}
                color={activeFilters.includes(filter) ? "primary" : "default"}
                onClick={() => handleFilterClick(filter)}
                sx={{
                  borderRadius: 1.5,
                  fontSize: '0.75rem',
                  height: 28,
                  '&:hover': {
                    backgroundColor: activeFilters.includes(filter)
                      ? alpha(theme.palette.primary.dark, 0.9)
                      : alpha(theme.palette.primary.main, 0.1),
                  },
                  transition: 'all 0.2s ease-in-out',
                }}
              />
            ))}
          </Stack>
        </Box>
      )}

      {/* Active Filters Display */}
      {activeFilters.length > 0 && (
        <Box sx={{ mt: 1 }}>
          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
            {activeFilters.map((filter) => (
              <Chip
                key={filter}
                label={filter}
                size="small"
                color="primary"
                onDelete={() => handleFilterClick(filter)}
                sx={{
                  borderRadius: 1.5,
                  fontSize: '0.75rem',
                  height: 24,
                  '& .MuiChip-deleteIcon': {
                    fontSize: '1rem',
                    '&:hover': {
                      color: theme.palette.primary.contrastText,
                    }
                  }
                }}
              />
            ))}
          </Stack>
        </Box>
      )}
    </Box>
  );
};

export default SearchInput;