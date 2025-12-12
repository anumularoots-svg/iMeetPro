// components/invitations/EmailValidation.jsx
import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Button,
  TextField,
  Chip,
  Alert,
  Divider,
  Card,
  CardContent,
  Grid,
  Switch,
  FormControlLabel,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  useTheme,
  alpha
} from '@mui/material';
import {
  CheckCircle as ValidIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  Add as AddIcon,
  FilterList as FilterIcon,
  SelectAll as SelectAllIcon,
  Clear as ClearIcon
} from '@mui/icons-material';

const EmailValidation = ({ 
  emails, 
  onValidatedEmails, 
  onRemoveEmail, 
  showAdvancedValidation = true 
}) => {
  const theme = useTheme();
  const [validationResults, setValidationResults] = useState([]);
  const [filterType, setFilterType] = useState('all'); // 'all', 'valid', 'invalid', 'warning'
  const [editingEmail, setEditingEmail] = useState(null);
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [newEmail, setNewEmail] = useState('');
  const [selectAll, setSelectAll] = useState(false);
  const [selectedEmails, setSelectedEmails] = useState(new Set());

  // Enhanced email validation
  const validateEmail = (email, allEmails = []) => {
    const result = {
      email: email,
      isValid: false,
      level: 'error',
      issues: [],
      suggestions: []
    };

    // Basic format validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      result.issues.push('Invalid email format');
      result.level = 'error';
      return result;
    }

    // More detailed validation
    const parts = email.split('@');
    if (parts.length !== 2) {
      result.issues.push('Email must contain exactly one @ symbol');
      result.level = 'error';
      return result;
    }

    const [localPart, domain] = parts;

    // Local part validation
    if (localPart.length === 0) {
      result.issues.push('Email cannot start with @');
      result.level = 'error';
      return result;
    }

    if (localPart.length > 64) {
      result.issues.push('Local part too long (max 64 characters)');
      result.level = 'error';
      return result;
    }

    // Domain validation
    if (domain.length === 0) {
      result.issues.push('Domain cannot be empty');
      result.level = 'error';
      return result;
    }

    if (domain.length > 253) {
      result.issues.push('Domain too long (max 253 characters)');
      result.level = 'error';
      return result;
    }

    const domainParts = domain.split('.');
    if (domainParts.length < 2) {
      result.issues.push('Domain must contain at least one dot');
      result.level = 'error';
      return result;
    }

    const tld = domainParts[domainParts.length - 1];
    if (tld.length < 2) {
      result.issues.push('Top-level domain too short');
      result.level = 'error';
      return result;
    }

    // Check for common issues
    if (email.includes('..')) {
      result.issues.push('Contains consecutive dots');
      result.level = 'error';
      return result;
    }

    if (email.startsWith('.') || email.endsWith('.')) {
      result.issues.push('Cannot start or end with a dot');
      result.level = 'error';
      return result;
    }

    // Advanced validations (warnings)
    const warnings = [];
    
    // Check for common typos in domains
    const commonDomains = ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com'];
    const similarDomains = {
      'gmai.com': 'gmail.com',
      'gmial.com': 'gmail.com',
      'yahooo.com': 'yahoo.com',
      'outlok.com': 'outlook.com',
      'hotmial.com': 'hotmail.com'
    };

    if (similarDomains[domain]) {
      warnings.push(`Did you mean ${similarDomains[domain]}?`);
      result.suggestions.push(`Change domain to ${similarDomains[domain]}`);
    }

    // Check for duplicates
    const duplicateCount = allEmails.filter(e => e.toLowerCase() === email.toLowerCase()).length;
    if (duplicateCount > 1) {
      warnings.push('Duplicate email detected');
    }

    // Check for suspicious patterns
    if (email.includes('+')) {
      warnings.push('Contains + symbol (alias email)');
    }

    if (localPart.length < 3) {
      warnings.push('Very short email address');
    }

    // If no errors, check warnings
    if (warnings.length > 0) {
      result.isValid = true;
      result.level = 'warning';
      result.issues = warnings;
    } else {
      result.isValid = true;
      result.level = 'valid';
      result.issues = ['Valid email address'];
    }

    return result;
  };

  // Validate all emails when they change
  useEffect(() => {
    const results = emails.map(email => validateEmail(email, emails));
    setValidationResults(results);
    
    // Notify parent of valid emails
    const validEmails = results
      .filter(result => result.isValid)
      .map(result => result.email);
    
    if (onValidatedEmails) {
      onValidatedEmails(validEmails);
    }
  }, [emails]);

  const filteredResults = validationResults.filter(result => {
    switch (filterType) {
      case 'valid':
        return result.level === 'valid';
      case 'invalid':
        return result.level === 'error';
      case 'warning':
        return result.level === 'warning';
      default:
        return true;
    }
  });

  const handleEditEmail = (result) => {
    setEditingEmail(result);
    setNewEmail(result.email);
    setEditDialogOpen(true);
  };

  const handleSaveEdit = () => {
    if (editingEmail && newEmail !== editingEmail.email) {
      // Replace the old email with new email
      const updatedEmails = emails.map(email => 
        email === editingEmail.email ? newEmail : email
      );
      
      // Re-validate
      const newResults = updatedEmails.map(email => validateEmail(email, updatedEmails));
      setValidationResults(newResults);
      
      // Notify parent
      if (onValidatedEmails) {
        const validEmails = newResults
          .filter(result => result.isValid)
          .map(result => result.email);
        onValidatedEmails(validEmails);
      }
    }
    
    setEditDialogOpen(false);
    setEditingEmail(null);
    setNewEmail('');
  };

  const handleSelectEmail = (email) => {
    const newSelected = new Set(selectedEmails);
    if (newSelected.has(email)) {
      newSelected.delete(email);
    } else {
      newSelected.add(email);
    }
    setSelectedEmails(newSelected);
  };

  const handleSelectAll = () => {
    if (selectAll) {
      setSelectedEmails(new Set());
    } else {
      setSelectedEmails(new Set(filteredResults.map(r => r.email)));
    }
    setSelectAll(!selectAll);
  };

  const handleBulkDelete = () => {
    selectedEmails.forEach(email => {
      if (onRemoveEmail) {
        onRemoveEmail(email);
      }
    });
    setSelectedEmails(new Set());
    setSelectAll(false);
  };

  const getIconByLevel = (level) => {
    switch (level) {
      case 'valid':
        return <ValidIcon color="success" />;
      case 'warning':
        return <WarningIcon color="warning" />;
      case 'error':
        return <ErrorIcon color="error" />;
      default:
        return <ErrorIcon color="error" />;
    }
  };

  const getCountByLevel = (level) => {
    return validationResults.filter(result => result.level === level).length;
  };

  return (
    <Box>
      {/* Summary Cards */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ 
            bgcolor: alpha(theme.palette.success.main, 0.1),
            border: `1px solid ${alpha(theme.palette.success.main, 0.2)}`
          }}>
            <CardContent sx={{ textAlign: 'center', py: 2 }}>
              <ValidIcon sx={{ fontSize: 32, color: 'success.main', mb: 1 }} />
              <Typography variant="h4" color="success.main" fontWeight="bold">
                {getCountByLevel('valid')}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Valid Emails
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ 
            bgcolor: alpha(theme.palette.warning.main, 0.1),
            border: `1px solid ${alpha(theme.palette.warning.main, 0.2)}`
          }}>
            <CardContent sx={{ textAlign: 'center', py: 2 }}>
              <WarningIcon sx={{ fontSize: 32, color: 'warning.main', mb: 1 }} />
              <Typography variant="h4" color="warning.main" fontWeight="bold">
                {getCountByLevel('warning')}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Warnings
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ 
            bgcolor: alpha(theme.palette.error.main, 0.1),
            border: `1px solid ${alpha(theme.palette.error.main, 0.2)}`
          }}>
            <CardContent sx={{ textAlign: 'center', py: 2 }}>
              <ErrorIcon sx={{ fontSize: 32, color: 'error.main', mb: 1 }} />
              <Typography variant="h4" color="error.main" fontWeight="bold">
                {getCountByLevel('error')}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Invalid
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ 
            bgcolor: alpha(theme.palette.info.main, 0.1),
            border: `1px solid ${alpha(theme.palette.info.main, 0.2)}`
          }}>
            <CardContent sx={{ textAlign: 'center', py: 2 }}>
              <FilterIcon sx={{ fontSize: 32, color: 'info.main', mb: 1 }} />
              <Typography variant="h4" color="info.main" fontWeight="bold">
                {validationResults.length}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Total Emails
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Filter Controls */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 2 }}>
            <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
              <Chip
                label="All"
                variant={filterType === 'all' ? 'filled' : 'outlined'}
                onClick={() => setFilterType('all')}
                color={filterType === 'all' ? 'primary' : 'default'}
              />
              <Chip
                label="Valid"
                variant={filterType === 'valid' ? 'filled' : 'outlined'}
                onClick={() => setFilterType('valid')}
                color={filterType === 'valid' ? 'success' : 'default'}
              />
              <Chip
                label="Warnings"
                variant={filterType === 'warning' ? 'filled' : 'outlined'}
                onClick={() => setFilterType('warning')}
                color={filterType === 'warning' ? 'warning' : 'default'}
              />
              <Chip
                label="Invalid"
                variant={filterType === 'invalid' ? 'filled' : 'outlined'}
                onClick={() => setFilterType('invalid')}
                color={filterType === 'invalid' ? 'error' : 'default'}
              />
            </Box>

            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button
                startIcon={<SelectAllIcon />}
                onClick={handleSelectAll}
                size="small"
              >
                {selectAll ? 'Deselect All' : 'Select All'}
              </Button>
              
              {selectedEmails.size > 0 && (
                <Button
                  startIcon={<DeleteIcon />}
                  onClick={handleBulkDelete}
                  color="error"
                  size="small"
                >
                  Delete Selected ({selectedEmails.size})
                </Button>
              )}
            </Box>
          </Box>
        </CardContent>
      </Card>

      {/* Email List */}
      <Paper elevation={2} sx={{ borderRadius: 2 }}>
        {filteredResults.length === 0 ? (
          <Box sx={{ p: 4, textAlign: 'center' }}>
            <Typography variant="h6" color="text.secondary">
              No emails match the current filter
            </Typography>
          </Box>
        ) : (
          <List>
            {filteredResults.map((result, index) => (
              <React.Fragment key={result.email}>
                <ListItem
                  sx={{
                    bgcolor: selectedEmails.has(result.email) 
                      ? alpha(theme.palette.primary.main, 0.1) 
                      : 'transparent',
                    '&:hover': {
                      bgcolor: alpha(theme.palette.action.hover, 0.1)
                    }
                  }}
                >
                  <ListItemIcon>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Switch
                        checked={selectedEmails.has(result.email)}
                        onChange={() => handleSelectEmail(result.email)}
                        size="small"
                      />
                      {getIconByLevel(result.level)}
                    </Box>
                  </ListItemIcon>
                  
                  <ListItemText
                    primary={
                      <Typography 
                        variant="body1" 
                        sx={{ 
                          fontWeight: result.level === 'error' ? 'normal' : 'medium',
                          color: result.level === 'error' ? 'error.main' : 'text.primary'
                        }}
                      >
                        {result.email}
                      </Typography>
                    }
                    secondary={
                      <Box>
                        {result.issues.map((issue, idx) => (
                          <Typography 
                            key={idx}
                            variant="caption" 
                            sx={{ 
                              display: 'block',
                              color: result.level === 'error' ? 'error.main' : 
                                     result.level === 'warning' ? 'warning.main' : 'success.main'
                            }}
                          >
                            • {issue}
                          </Typography>
                        ))}
                        {result.suggestions.map((suggestion, idx) => (
                          <Typography 
                            key={idx}
                            variant="caption" 
                            sx={{ 
                              display: 'block',
                              color: 'info.main',
                              fontStyle: 'italic'
                            }}
                          >
                            💡 {suggestion}
                          </Typography>
                        ))}
                      </Box>
                    }
                  />
                  
                  <ListItemSecondaryAction>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      {showAdvancedValidation && (
                        <IconButton
                          edge="end"
                          onClick={() => handleEditEmail(result)}
                          size="small"
                        >
                          <EditIcon />
                        </IconButton>
                      )}
                      <IconButton
                        edge="end"
                        onClick={() => onRemoveEmail && onRemoveEmail(result.email)}
                        size="small"
                        color="error"
                      >
                        <DeleteIcon />
                      </IconButton>
                    </Box>
                  </ListItemSecondaryAction>
                </ListItem>
                
                {index < filteredResults.length - 1 && <Divider />}
              </React.Fragment>
            ))}
          </List>
        )}
      </Paper>

      {/* Edit Email Dialog */}
      <Dialog open={editDialogOpen} onClose={() => setEditDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Edit Email Address</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Email Address"
            value={newEmail}
            onChange={(e) => setNewEmail(e.target.value)}
            variant="outlined"
            sx={{ mt: 2 }}
            error={newEmail && !validateEmail(newEmail).isValid}
            helperText={
              newEmail && !validateEmail(newEmail).isValid 
                ? 'Please enter a valid email address'
                : ''
            }
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditDialogOpen(false)}>Cancel</Button>
          <Button 
            onClick={handleSaveEdit}
            variant="contained"
            disabled={!newEmail || !validateEmail(newEmail).isValid}
          >
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default EmailValidation;