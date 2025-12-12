// src/components/chat/ChatSearch.jsx
import React, { useState, useEffect, useMemo } from 'react';
import {
  Box,
  TextField,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  Avatar,
  Chip,
  IconButton,
  InputAdornment,
  Collapse,
  Divider,
  Paper,
  useTheme,
  Tooltip
} from '@mui/material';
import {
  Search as SearchIcon,
  Clear as ClearIcon,
  FilterList as FilterIcon,
  Person as PersonIcon,
  CalendarToday as DateIcon,
  AttachFile as FileIcon,
  ExpandMore as ExpandIcon,
  ExpandLess as CollapseIcon
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import { formatDistanceToNow } from 'date-fns';

const SearchContainer = styled(Box)(({ theme }) => ({
  padding: theme.spacing(2),
  background: `linear-gradient(135deg, ${theme.palette.background.paper} 0%, ${theme.palette.grey[50]} 100%)`,
  borderRadius: theme.spacing(1)
}));

const SearchField = styled(TextField)(({ theme }) => ({
  '& .MuiOutlinedInput-root': {
    borderRadius: theme.spacing(3),
    background: theme.palette.background.paper,
    '& fieldset': {
      borderColor: theme.palette.divider
    },
    '&:hover fieldset': {
      borderColor: theme.palette.primary.main
    },
    '&.Mui-focused fieldset': {
      borderColor: theme.palette.primary.main,
      borderWidth: 2
    }
  }
}));

const FilterChip = styled(Chip)(({ theme, active }) => ({
  margin: theme.spacing(0.5),
  background: active 
    ? `linear-gradient(45deg, ${theme.palette.primary.main} 30%, ${theme.palette.primary.light} 90%)`
    : theme.palette.background.paper,
  color: active ? theme.palette.primary.contrastText : theme.palette.text.primary,
  border: `1px solid ${active ? theme.palette.primary.main : theme.palette.divider}`,
  fontWeight: active ? 600 : 400,
  '&:hover': {
    background: active 
      ? `linear-gradient(45deg, ${theme.palette.primary.dark} 30%, ${theme.palette.primary.main} 90%)`
      : theme.palette.action.hover,
    transform: 'scale(1.02)'
  },
  transition: 'all 0.3s ease'
}));

const SearchResult = styled(ListItem)(({ theme }) => ({
  borderRadius: theme.spacing(1),
  marginBottom: theme.spacing(1),
  background: theme.palette.background.paper,
  border: `1px solid ${theme.palette.divider}`,
  transition: 'all 0.3s ease',
  '&:hover': {
    background: theme.palette.action.hover,
    transform: 'translateY(-1px)',
    boxShadow: theme.shadows[2]
  },
  '&:last-child': {
    marginBottom: 0
  }
}));

const HighlightedText = styled('span')(({ theme }) => ({
  background: `linear-gradient(45deg, ${theme.palette.warning.light} 30%, ${theme.palette.warning.main} 90%)`,
  color: theme.palette.warning.contrastText,
  padding: theme.spacing(0.25, 0.5),
  borderRadius: theme.spacing(0.5),
  fontWeight: 600
}));

const ChatSearch = ({ 
  messages = [], 
  onSearchResults,
  onMessageSelect,
  placeholder = "Search messages..." 
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedFilters, setSelectedFilters] = useState([]);
  const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
  const [searchResults, setSearchResults] = useState([]);
  const theme = useTheme();

  const availableFilters = [
    { id: 'text', label: 'Text Messages', icon: <SearchIcon /> },
    { id: 'files', label: 'Files', icon: <FileIcon /> },
    { id: 'images', label: 'Images', icon: <FileIcon /> },
    { id: 'today', label: 'Today', icon: <DateIcon /> },
    { id: 'yesterday', label: 'Yesterday', icon: <DateIcon /> },
    { id: 'this-week', label: 'This Week', icon: <DateIcon /> },
    { id: 'my-messages', label: 'My Messages', icon: <PersonIcon /> }
  ];

  // Search functionality
  const filteredMessages = useMemo(() => {
    if (!searchQuery.trim() && selectedFilters.length === 0) {
      return [];
    }

    let results = messages;

    // Apply text search
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      results = results.filter(message => 
        message.message.toLowerCase().includes(query) ||
        message.userName.toLowerCase().includes(query)
      );
    }

    // Apply filters
    selectedFilters.forEach(filter => {
      switch (filter) {
        case 'text':
          results = results.filter(msg => msg.messageType === 'text');
          break;
        case 'files':
          results = results.filter(msg => msg.messageType === 'file');
          break;
        case 'images':
          results = results.filter(msg => 
            msg.messageType === 'file' && 
            msg.message.match(/\.(jpg|jpeg|png|gif|webp)$/i)
          );
          break;
        case 'today':
          const today = new Date().toDateString();
          results = results.filter(msg => 
            new Date(msg.timestamp).toDateString() === today
          );
          break;
        case 'yesterday':
          const yesterday = new Date();
          yesterday.setDate(yesterday.getDate() - 1);
          results = results.filter(msg => 
            new Date(msg.timestamp).toDateString() === yesterday.toDateString()
          );
          break;
        case 'this-week':
          const weekAgo = new Date();
          weekAgo.setDate(weekAgo.getDate() - 7);
          results = results.filter(msg => 
            new Date(msg.timestamp) >= weekAgo
          );
          break;
        case 'my-messages':
          // This would need currentUser context
          results = results.filter(msg => msg.userId === 'current-user-id');
          break;
        default:
          break;
      }
    });

    return results.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  }, [messages, searchQuery, selectedFilters]);

  useEffect(() => {
    setSearchResults(filteredMessages);
    if (onSearchResults) {
      onSearchResults(filteredMessages);
    }
  }, [filteredMessages, onSearchResults]);

  const handleFilterToggle = (filterId) => {
    setSelectedFilters(prev => 
      prev.includes(filterId)
        ? prev.filter(id => id !== filterId)
        : [...prev, filterId]
    );
  };

  const clearSearch = () => {
    setSearchQuery('');
    setSelectedFilters([]);
    setSearchResults([]);
  };

  const highlightSearchText = (text, query) => {
    if (!query.trim()) return text;
    
    const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
    const parts = text.split(regex);
    
    return parts.map((part, index) => 
      regex.test(part) ? (
        <HighlightedText key={index}>{part}</HighlightedText>
      ) : (
        part
      )
    );
  };

  const getMessageIcon = (message) => {
    if (message.messageType === 'file') {
      const fileName = message.message.toLowerCase();
      if (fileName.match(/\.(jpg|jpeg|png|gif|webp)$/)) {
        return '🖼️';
      } else if (fileName.endsWith('.pdf')) {
        return '📄';
      } else if (fileName.match(/\.(doc|docx)$/)) {
        return '📝';
      } else {
        return '📎';
      }
    }
    return '💬';
  };

  return (
    <SearchContainer>
      {/* Search Input */}
      <SearchField
        fullWidth
        variant="outlined"
        placeholder={placeholder}
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <SearchIcon color="action" />
            </InputAdornment>
          ),
          endAdornment: (
            <InputAdornment position="end">
              {(searchQuery || selectedFilters.length > 0) && (
                <Tooltip title="Clear search">
                  <IconButton onClick={clearSearch} size="small">
                    <ClearIcon />
                  </IconButton>
                </Tooltip>
              )}
              <Tooltip title="Advanced filters">
                <IconButton 
                  onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
                  size="small"
                  color={selectedFilters.length > 0 ? "primary" : "default"}
                >
                  <FilterIcon />
                </IconButton>
              </Tooltip>
            </InputAdornment>
          )
        }}
      />

      {/* Filter Chips */}
      <Collapse in={showAdvancedFilters}>
        <Box mt={2}>
          <Box display="flex" alignItems="center" gap={1} mb={1}>
            <FilterIcon color="primary" />
            <Typography variant="subtitle2" color="primary" fontWeight="bold">
              Filters
            </Typography>
          </Box>
          
          <Box display="flex" flexWrap="wrap" gap={0.5}>
            {availableFilters.map((filter) => (
              <FilterChip
                key={filter.id}
                icon={filter.icon}
                label={filter.label}
                clickable
                onClick={() => handleFilterToggle(filter.id)}
                active={selectedFilters.includes(filter.id)}
                size="small"
              />
            ))}
          </Box>
        </Box>
      </Collapse>

      {/* Search Results Count */}
      {(searchQuery || selectedFilters.length > 0) && (
        <>
          <Divider sx={{ my: 2 }} />
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
            <Typography variant="body2" color="text.secondary">
              {searchResults.length} {searchResults.length === 1 ? 'result' : 'results'} found
            </Typography>
            {searchResults.length > 0 && (
              <IconButton 
                size="small" 
                onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
              >
                {showAdvancedFilters ? <CollapseIcon /> : <ExpandIcon />}
              </IconButton>
            )}
          </Box>
        </>
      )}

      {/* Search Results */}
      {searchResults.length > 0 && (
        <Paper 
          variant="outlined" 
          sx={{ 
            maxHeight: 300, 
            overflow: 'auto',
            background: 'transparent',
            '&::-webkit-scrollbar': {
              width: 6
            },
            '&::-webkit-scrollbar-track': {
              background: theme.palette.grey[100],
              borderRadius: 3
            },
            '&::-webkit-scrollbar-thumb': {
              background: theme.palette.primary.light,
              borderRadius: 3
            }
          }}
        >
          <List dense>
            {searchResults.map((message) => (
              <SearchResult
                key={message.id}
                button
                onClick={() => onMessageSelect && onMessageSelect(message)}
              >
                <ListItemAvatar>
                  <Avatar
                    src={message.userAvatar}
                    sx={{
                      width: 32,
                      height: 32,
                      background: `linear-gradient(45deg, ${theme.palette.primary.main} 30%, ${theme.palette.primary.light} 90%)`,
                      fontSize: '0.75rem'
                    }}
                  >
                    {message.userName.charAt(0)}
                  </Avatar>
                </ListItemAvatar>
                
                <ListItemText
                  primary={
                    <Box display="flex" alignItems="center" gap={1}>
                      <Typography variant="body2" fontWeight="bold">
                        {highlightSearchText(message.userName, searchQuery)}
                      </Typography>
                      <Typography variant="caption" sx={{ opacity: 0.7 }}>
                        {getMessageIcon(message)}
                      </Typography>
                    </Box>
                  }
                  secondary={
                    <Box>
                      <Typography 
                        variant="body2" 
                        color="text.primary"
                        sx={{ 
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          display: '-webkit-box',
                          WebkitLineClamp: 2,
                          WebkitBoxOrient: 'vertical'
                        }}
                      >
                        {highlightSearchText(message.message, searchQuery)}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {formatDistanceToNow(new Date(message.timestamp), { addSuffix: true })}
                      </Typography>
                    </Box>
                  }
                />
              </SearchResult>
            ))}
          </List>
        </Paper>
      )}

      {/* No Results */}
      {(searchQuery || selectedFilters.length > 0) && searchResults.length === 0 && (
        <Box 
          display="flex" 
          flexDirection="column" 
          alignItems="center" 
          py={3}
          color="text.secondary"
        >
          <SearchIcon sx={{ fontSize: 48, mb: 1, opacity: 0.5 }} />
          <Typography variant="h6" gutterBottom>
            No messages found
          </Typography>
          <Typography variant="body2" textAlign="center">
            Try adjusting your search terms or filters
          </Typography>
        </Box>
      )}
    </SearchContainer>
  );
};

export default ChatSearch;