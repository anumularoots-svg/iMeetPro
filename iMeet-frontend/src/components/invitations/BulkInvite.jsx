// COMPLETE FIXED: src/components/invitations/BulkInvite.jsx - Add participants only, no email sending

import React, { useState, useCallback, useRef } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Typography,
  Box,
  Chip,
  Alert,
  CircularProgress,
  IconButton,
  Divider,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Paper,
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Tabs,
  Tab,
  LinearProgress
} from '@mui/material';
import {
  Close,
  CloudUpload,
  Email,
  Person,
  Delete,
  Add,
  CheckCircle,
  Warning,
  Info,
  ExpandMore,
  Upload,
  Download,
  ContentCopy,
  GroupAdd,
  FileUpload,
  Visibility,
  VisibilityOff,
  Clear,
  ArrowForward
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import * as XLSX from 'xlsx';
import Papa from 'papaparse';

const StyledDialog = styled(Dialog)(({ theme }) => ({
  '& .MuiDialog-paper': {
    borderRadius: theme.spacing(3),
    maxWidth: '900px',
    width: '95%',
    maxHeight: '85vh'
  }
}));

const UploadArea = styled(Box)(({ theme }) => ({
  border: `2px dashed ${theme.palette.primary.main}`,
  borderRadius: theme.spacing(2),
  padding: theme.spacing(4),
  textAlign: 'center',
  cursor: 'pointer',
  transition: 'all 0.3s ease',
  backgroundColor: theme.palette.grey[50],
  '&:hover': {
    borderColor: theme.palette.primary.dark,
    backgroundColor: theme.palette.primary.light + '10',
  },
  '&.dragover': {
    borderColor: theme.palette.success.main,
    backgroundColor: theme.palette.success.light + '20',
  }
}));

const EmailChip = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.25),
  '& .MuiChip-deleteIcon': {
    color: theme.palette.text.secondary,
    '&:hover': {
      color: theme.palette.error.main,
    },
  },
}));

// Custom TabPanel component
function CustomTabPanel({ children, value, index, ...other }) {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`bulk-invite-tabpanel-${index}`}
      aria-labelledby={`bulk-invite-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ pt: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

function BulkInvite({ open, onClose, meetingId, onInvitesSent, meetingTitle = "Meeting" }) {
  const [currentTab, setCurrentTab] = useState(0);
  const [emailInput, setEmailInput] = useState('');
  const [validEmails, setValidEmails] = useState([]);
  const [invalidEmails, setInvalidEmails] = useState([]);
  const [duplicateEmails, setDuplicateEmails] = useState([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [dragOver, setDragOver] = useState(false);
  const [fileUploading, setFileUploading] = useState(false);
  const [uploadedFileName, setUploadedFileName] = useState('');
  const [uploadProgress, setUploadProgress] = useState(0);
  const [showInvalidEmails, setShowInvalidEmails] = useState(false);
  
  const fileInputRef = useRef(null);

  // Email validation function
  const isValidEmail = (email) => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email.trim());
  };

  // Parse emails from text input with enhanced validation
  const parseEmailsFromText = useCallback((text) => {
    if (!text.trim()) return { valid: [], invalid: [], duplicates: [] };

    // Enhanced splitting regex to handle various delimiters
    const emails = text
      .split(/[,;\n\r\t\s]+/)
      .map(email => email.trim())
      .filter(email => email.length > 0)
      .map(email => email.toLowerCase()); // Normalize to lowercase

    const valid = [];
    const invalid = [];
    const duplicates = [];
    const seenEmails = new Set();

    emails.forEach(email => {
      if (isValidEmail(email)) {
        if (seenEmails.has(email)) {
          duplicates.push(email);
        } else {
          seenEmails.add(email);
          valid.push(email);
        }
      } else {
        invalid.push(email);
      }
    });

    return { valid, invalid, duplicates };
  }, []);

  // Add emails from text input with enhanced feedback
  const addEmailsFromText = () => {
    if (!emailInput.trim()) {
      setError("Please enter some email addresses");
      return;
    }

    const { valid, invalid, duplicates } = parseEmailsFromText(emailInput);
    
    // Merge with existing emails, avoiding duplicates
    const existingEmails = new Set(validEmails.map(email => email.toLowerCase()));
    const newValidEmails = valid.filter(email => !existingEmails.has(email));
    const alreadyExisting = valid.filter(email => existingEmails.has(email));
    
    setValidEmails(prev => [...prev, ...newValidEmails]);
    setInvalidEmails(invalid);
    setDuplicateEmails([...duplicates, ...alreadyExisting]);
    setEmailInput('');
    
    // Provide detailed feedback
    let message = '';
    if (newValidEmails.length > 0) {
      message += `Added ${newValidEmails.length} new email(s). `;
    }
    if (alreadyExisting.length > 0) {
      message += `${alreadyExisting.length} email(s) already in list. `;
    }
    if (duplicates.length > 0) {
      message += `${duplicates.length} duplicate(s) in input. `;
    }
    
    if (newValidEmails.length > 0) {
      setSuccessMessage(message);
      setTimeout(() => setSuccessMessage(''), 4000);
    }
    
    if (invalid.length > 0) {
      setError(`${invalid.length} invalid email(s) were skipped`);
      setTimeout(() => setError(''), 5000);
    }
  };

  // Remove email from list
  const removeEmail = (emailToRemove) => {
    setValidEmails(prev => prev.filter(email => email !== emailToRemove));
  };

  // Clear all emails
  const clearAllEmails = () => {
    setValidEmails([]);
    setInvalidEmails([]);
    setDuplicateEmails([]);
    setError('');
    setSuccessMessage('');
  };

  // Enhanced file upload handler with progress tracking
  const handleFileUpload = async (file) => {
    setFileUploading(true);
    setError('');
    setUploadProgress(0);
    
    try {
      const fileExtension = file.name.split('.').pop().toLowerCase();
      setUploadedFileName(file.name);
      
      let emailsFromFile = [];
      
      setUploadProgress(20);
      
      if (fileExtension === 'csv') {
        // Parse CSV file
        const text = await file.text();
        setUploadProgress(40);
        
        const results = Papa.parse(text, {
          header: true,
          skipEmptyLines: true,
          transformHeader: (header) => header.trim().toLowerCase()
        });
        
        setUploadProgress(60);
        
        // Extract emails from various possible column names
        const emailColumns = ['email', 'email address', 'e-mail', 'emails', 'mail', 'email_address'];
        
        results.data.forEach(row => {
          emailColumns.forEach(column => {
            if (row[column] && typeof row[column] === 'string') {
              const emails = row[column].split(/[,;]/).map(e => e.trim());
              emailsFromFile.push(...emails);
            }
          });
          
          // Also check if any cell contains an email pattern
          Object.values(row).forEach(value => {
            if (typeof value === 'string' && value.includes('@') && isValidEmail(value)) {
              emailsFromFile.push(value.trim());
            }
          });
        });
        
      } else if (['xlsx', 'xls'].includes(fileExtension)) {
        // Parse Excel file
        const arrayBuffer = await file.arrayBuffer();
        setUploadProgress(40);
        
        const workbook = XLSX.read(arrayBuffer, { type: 'array' });
        setUploadProgress(60);
        
        // Process all sheets
        workbook.SheetNames.forEach(sheetName => {
          const worksheet = workbook.Sheets[sheetName];
          const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
          
          jsonData.forEach(row => {
            if (Array.isArray(row)) {
              row.forEach(cell => {
                if (typeof cell === 'string' && cell.includes('@') && isValidEmail(cell)) {
                  emailsFromFile.push(cell.trim());
                }
              });
            }
          });
        });
        
      } else if (fileExtension === 'txt') {
        // Parse text file
        const text = await file.text();
        setUploadProgress(60);
        
        emailsFromFile = text
          .split(/[\n\r,;]/)
          .map(email => email.trim())
          .filter(email => email.length > 0);
      } else {
        throw new Error('Unsupported file format. Please use CSV, Excel (.xlsx, .xls), or TXT files.');
      }
      
      setUploadProgress(80);
      
      // Process extracted emails
      const { valid, invalid, duplicates } = parseEmailsFromText(emailsFromFile.join('\n'));
      
      if (valid.length === 0) {
        throw new Error('No valid emails found in the file. Please check the file format and content.');
      }
      
      // Merge with existing emails
      const existingEmails = new Set(validEmails.map(email => email.toLowerCase()));
      const newValidEmails = valid.filter(email => !existingEmails.has(email));
      const alreadyExisting = valid.filter(email => existingEmails.has(email));
      
      setValidEmails(prev => [...prev, ...newValidEmails]);
      setInvalidEmails(invalid);
      setDuplicateEmails(duplicates);
      
      setUploadProgress(100);
      
      let message = `Successfully imported ${newValidEmails.length} new email(s) from ${file.name}`;
      if (alreadyExisting.length > 0) {
        message += `. ${alreadyExisting.length} were already in the list`;
      }
      
      setSuccessMessage(message);
      setTimeout(() => setSuccessMessage(''), 5000);
      
      if (invalid.length > 0) {
        setError(`${invalid.length} invalid email(s) were skipped from the file`);
        setTimeout(() => setError(''), 5000);
      }
      
    } catch (error) {
      console.error('File upload error:', error);
      setError(`Failed to process file: ${error.message}`);
      setUploadProgress(0);
    } finally {
      setFileUploading(false);
      setTimeout(() => setUploadProgress(0), 2000);
    }
  };

  // Drag and drop handlers
  const handleDragOver = (e) => {
    e.preventDefault();
    setDragOver(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    setDragOver(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      const file = files[0];
      const allowedTypes = ['text/csv', 'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'text/plain'];
      const allowedExtensions = ['csv', 'xlsx', 'xls', 'txt'];
      const fileExtension = file.name.split('.').pop().toLowerCase();
      
      if (allowedTypes.includes(file.type) || allowedExtensions.includes(fileExtension)) {
        handleFileUpload(file);
      } else {
        setError('Invalid file type. Please upload CSV, Excel, or TXT files only.');
      }
    }
  };

  // File input change handler
  const handleFileInputChange = (e) => {
    const file = e.target.files[0];
    if (file) {
      handleFileUpload(file);
    }
  };

  // Add participants to meeting (no email sending)
  const handleAddParticipants = async () => {
    if (validEmails.length === 0) {
      setError("Please add some valid email addresses");
      return;
    }

    setIsProcessing(true);
    setError("");

    try {
      console.log("📧 Adding participants to meeting:", validEmails);
      
      // Call the callback to add emails to the parent component
      if (onInvitesSent) {
        onInvitesSent(validEmails);
      }

      setSuccessMessage(`Successfully added ${validEmails.length} participants to the meeting!`);
      
      // Auto-close after success
      setTimeout(() => {
        onClose();
      }, 1500);
      
    } catch (error) {
      console.error("❌ Error adding participants:", error);
      setError("Failed to add participants. Please try again.");
    } finally {
      setIsProcessing(false);
    }
  };

  // Sample template download
  const downloadSampleTemplate = () => {
    const sampleData = [
      ['Email Address', 'Name (Optional)', 'Department'],
      ['john.doe@example.com', 'John Doe', 'Engineering'],
      ['jane.smith@example.com', 'Jane Smith', 'Marketing'],
      ['team@company.com', 'Team Lead', 'Management'],
      ['support@vendor.com', 'Support Team', 'External']
    ];
    
    const ws = XLSX.utils.aoa_to_sheet(sampleData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Email Template');
    
    // Set column widths
    ws['!cols'] = [
      { wch: 25 }, // Email Address
      { wch: 15 }, // Name
      { wch: 15 }  // Department
    ];
    
    XLSX.writeFile(wb, 'bulk_invite_template.xlsx');
    
    setSuccessMessage('Sample template downloaded!');
    setTimeout(() => setSuccessMessage(''), 3000);
  };

  // Copy emails to clipboard
  const copyEmailsToClipboard = () => {
    const emailList = validEmails.join('\n');
    navigator.clipboard.writeText(emailList).then(() => {
      setSuccessMessage('Emails copied to clipboard!');
      setTimeout(() => setSuccessMessage(''), 3000);
    }).catch(() => {
      setError('Failed to copy emails to clipboard');
      setTimeout(() => setError(''), 3000);
    });
  };

  // Reset all data when closing
  const handleClose = () => {
    setCurrentTab(0);
    setEmailInput('');
    setValidEmails([]);
    setInvalidEmails([]);
    setDuplicateEmails([]);
    setError('');
    setSuccessMessage('');
    setUploadedFileName('');
    setUploadProgress(0);
    onClose();
  };

  return (
    <StyledDialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle sx={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center',
        pb: 1,
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white'
      }}>
        <Box>
          <Typography variant="h5" component="div">
            Add Participants to Meeting
          </Typography>
          <Typography variant="body2" sx={{ opacity: 0.9 }}>
            {meetingTitle}
          </Typography>
        </Box>
        <IconButton
          aria-label="close"
          onClick={handleClose}
          sx={{ color: 'white' }}
        >
          <Close />
        </IconButton>
      </DialogTitle>

      <DialogContent dividers sx={{ p: 0 }}>
        {/* Tabs */}
        <Tabs 
          value={currentTab} 
          onChange={(e, newValue) => setCurrentTab(newValue)}
          variant="fullWidth"
          sx={{ borderBottom: 1, borderColor: 'divider' }}
        >
          <Tab 
            label="Manual Entry" 
            icon={<Email />} 
            iconPosition="start"
          />
          <Tab 
            label="File Upload" 
            icon={<CloudUpload />} 
            iconPosition="start"
          />
          <Tab 
            label="Review & Add" 
            icon={<GroupAdd />} 
            iconPosition="start"
            disabled={validEmails.length === 0}
          />
        </Tabs>

        {/* Tab 0: Manual Entry */}
        <CustomTabPanel value={currentTab} index={0}>
          <Box sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Add Email Addresses
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
              Enter email addresses separated by commas, semicolons, spaces, or new lines
            </Typography>

            <TextField
              fullWidth
              multiline
              rows={6}
              label="Email Addresses"
              placeholder="john@example.com, jane@example.com, team@company.com&#10;support@vendor.com&#10;admin@domain.org"
              value={emailInput}
              onChange={(e) => setEmailInput(e.target.value)}
              sx={{ mb: 2 }}
              helperText="You can paste multiple emails at once"
            />

            <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
              <Button
                variant="contained"
                onClick={addEmailsFromText}
                disabled={!emailInput.trim()}
                startIcon={<Add />}
                sx={{ flexGrow: 1 }}
              >
                Add Emails ({emailInput ? parseEmailsFromText(emailInput).valid.length : 0} valid)
              </Button>
              <Button
                variant="outlined"
                onClick={() => setEmailInput('')}
                disabled={!emailInput.trim()}
                startIcon={<Clear />}
              >
                Clear
              </Button>
            </Box>

            {/* Valid Emails Display */}
            <Box sx={{ mb: 2 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                <Typography variant="subtitle2">
                  Valid Emails ({validEmails.length})
                </Typography>
                {validEmails.length > 0 && (
                  <Button
                    size="small"
                    startIcon={<ContentCopy />}
                    onClick={copyEmailsToClipboard}
                  >
                    Copy
                  </Button>
                )}
              </Box>
              <Box sx={{ 
                minHeight: 120,
                maxHeight: 200, 
                overflow: 'auto',
                border: '1px solid #ddd',
                borderRadius: 1,
                p: 1,
                bgcolor: 'grey.50'
              }}>
                {validEmails.map((email, index) => (
                  <EmailChip
                    key={index}
                    label={email}
                    onDelete={() => removeEmail(email)}
                    size="small"
                    color="primary"
                    variant="outlined"
                  />
                ))}
                {validEmails.length === 0 && (
                  <Typography variant="body2" color="text.secondary" sx={{ p: 2, textAlign: 'center' }}>
                    No valid emails added yet
                  </Typography>
                )}
              </Box>
            </Box>

            {/* Invalid Emails Display */}
            {invalidEmails.length > 0 && (
              <Box sx={{ mb: 2 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                  <Typography variant="subtitle2" color="error">
                    Invalid Emails ({invalidEmails.length})
                  </Typography>
                  <IconButton
                    size="small"
                    onClick={() => setShowInvalidEmails(!showInvalidEmails)}
                  >
                    {showInvalidEmails ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                </Box>
                {showInvalidEmails && (
                  <Box sx={{ 
                    maxHeight: 100, 
                    overflow: 'auto',
                    border: '1px solid #f44336',
                    borderRadius: 1,
                    p: 1,
                    bgcolor: 'error.light'
                  }}>
                    {invalidEmails.map((email, index) => (
                      <Chip
                        key={index}
                        label={email}
                        size="small"
                        color="error"
                        variant="outlined"
                        sx={{ m: 0.25 }}
                      />
                    ))}
                  </Box>
                )}
              </Box>
            )}

            {/* Duplicate Emails Display */}
            {duplicateEmails.length > 0 && (
              <Alert severity="warning" sx={{ mb: 2 }}>
                <Typography variant="body2">
                  <strong>Duplicates found:</strong> {duplicateEmails.length} email(s) were already in the list or appeared multiple times
                </Typography>
              </Alert>
            )}
          </Box>
        </CustomTabPanel>

        {/* Tab 1: File Upload */}
        <CustomTabPanel value={currentTab} index={1}>
          <Box sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Upload Email List
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
              Upload a CSV, Excel, or TXT file containing email addresses
            </Typography>

            <Grid container spacing={3}>
              <Grid item xs={12} md={8}>
                <UploadArea
                  className={dragOver ? 'dragover' : ''}
                  onDragOver={handleDragOver}
                  onDragLeave={handleDragLeave}
                  onDrop={handleDrop}
                  onClick={() => fileInputRef.current?.click()}
                >
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".csv,.xlsx,.xls,.txt"
                    onChange={handleFileInputChange}
                    style={{ display: 'none' }}
                  />
                  
                  {fileUploading ? (
                    <Box>
                      <CircularProgress sx={{ mb: 2 }} />
                      <Typography variant="h6">Processing file...</Typography>
                      {uploadedFileName && (
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                          {uploadedFileName}
                        </Typography>
                      )}
                      <LinearProgress variant="determinate" value={uploadProgress} sx={{ width: '80%', mx: 'auto' }} />
                    </Box>
                  ) : (
                    <Box>
                      <CloudUpload sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />
                      <Typography variant="h6" gutterBottom>
                        Drop files here or click to browse
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Supports CSV, Excel (.xlsx, .xls), and TXT files
                      </Typography>
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
                        Maximum file size: 10MB
                      </Typography>
                    </Box>
                  )}
                </UploadArea>
              </Grid>

              <Grid item xs={12} md={4}>
                <Paper sx={{ p: 2 }}>
                  <Typography variant="subtitle1" gutterBottom>
                    File Format Guidelines
                  </Typography>
                  <List dense>
                    <ListItem>
                      <ListItemIcon>
                        <Info color="primary" />
                      </ListItemIcon>
                      <ListItemText 
                        primary="CSV files"
                        secondary="Should have an 'email' column header"
                      />
                    </ListItem>
                    <ListItem>
                      <ListItemIcon>
                        <Info color="primary" />
                      </ListItemIcon>
                      <ListItemText 
                        primary="Excel files"
                        secondary="Emails will be detected automatically"
                      />
                    </ListItem>
                    <ListItem>
                      <ListItemIcon>
                        <Info color="primary" />
                      </ListItemIcon>
                      <ListItemText 
                        primary="TXT files"
                        secondary="One email per line or comma-separated"
                      />
                    </ListItem>
                  </List>
                  
                  <Button
                    fullWidth
                    variant="outlined"
                    startIcon={<Download />}
                    onClick={downloadSampleTemplate}
                    sx={{ mt: 2 }}
                  >
                    Download Sample Template
                  </Button>
                </Paper>
              </Grid>
            </Grid>

            {/* File Upload Results */}
            {validEmails.length > 0 && (
              <Box sx={{ mt: 3 }}>
                <Typography variant="subtitle2" gutterBottom>
                  Imported Emails ({validEmails.length})
                </Typography>
                <Box sx={{ 
                  maxHeight: 150, 
                  overflow: 'auto',
                  border: '1px solid #ddd',
                  borderRadius: 1,
                  p: 1,
                  bgcolor: 'grey.50'
                }}>
                  {validEmails.slice(0, 10).map((email, index) => (
                    <EmailChip
                      key={index}
                      label={email}
                      onDelete={() => removeEmail(email)}
                      size="small"
                      color="primary"
                      variant="outlined"
                    />
                  ))}
                  {validEmails.length > 10 && (
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
                      ... and {validEmails.length - 10} more
                    </Typography>
                  )}
                </Box>
              </Box>
            )}
          </Box>
        </CustomTabPanel>

        {/* Tab 2: Review & Add */}
        <CustomTabPanel value={currentTab} index={2}>
          <Box sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Review & Add Participants
            </Typography>

            <Paper sx={{ p: 3, mb: 3, bgcolor: 'grey.50' }}>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle2" color="primary">
                    MEETING DETAILS
                  </Typography>
                  <Typography variant="body1">
                    📧 Meeting: {meetingTitle}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    ID: {meetingId}
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle2" color="primary">
                    PARTICIPANTS TO ADD
                  </Typography>
                  <Typography variant="body1">
                    👥 {validEmails.length} email(s) selected
                  </Typography>
                  {validEmails.length > 0 && (
                    <Box sx={{ mt: 1 }}>
                      <Button
                        size="small"
                        startIcon={<ContentCopy />}
                        onClick={copyEmailsToClipboard}
                        sx={{ mr: 1 }}
                      >
                        Copy Emails
                      </Button>
                      <Button
                        size="small"
                        color="error"
                        startIcon={<Delete />}
                        onClick={clearAllEmails}
                      >
                        Clear All
                      </Button>
                    </Box>
                  )}
                </Grid>
              </Grid>
            </Paper>

            {validEmails.length > 0 ? (
              <Box>
                <Typography variant="subtitle1" gutterBottom>
                  These participants will be added to the meeting:
                </Typography>
                <Box sx={{ 
                  maxHeight: 350, 
                  overflow: 'auto',
                  border: '1px solid #ddd',
                  borderRadius: 1,
                  bgcolor: 'background.paper'
                }}>
                  <List dense>
                    {validEmails.map((email, index) => (
                      <ListItem 
                        key={index} 
                        divider={index < validEmails.length - 1}
                        secondaryAction={
                          <IconButton 
                            size="small" 
                            onClick={() => removeEmail(email)}
                            color="error"
                          >
                            <Delete fontSize="small" />
                          </IconButton>
                        }
                      >
                        <ListItemIcon>
                          <Email color="primary" />
                        </ListItemIcon>
                        <ListItemText 
                          primary={email}
                          secondary={`Participant ${index + 1}`}
                        />
                      </ListItem>
                    ))}
                  </List>
                </Box>
                
                <Alert severity="info" sx={{ mt: 2 }}>
                  <Typography variant="body2">
                    <strong>Note:</strong> These participants will be added to your meeting. 
                    Email invitations will be sent according to your meeting settings.
                  </Typography>
                </Alert>
              </Box>
            ) : (
              <Alert severity="warning">
                <Typography variant="body2">
                  No email addresses selected. Please go back and add some participants.
                </Typography>
              </Alert>
            )}
          </Box>
        </CustomTabPanel>

        {/* Error and Success Messages */}
        {error && (
          <Alert severity="error" sx={{ m: 2 }} onClose={() => setError('')}>
            {error}
          </Alert>
        )}

        {successMessage && (
          <Alert severity="success" sx={{ m: 2 }} onClose={() => setSuccessMessage('')}>
            {successMessage}
          </Alert>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2, bgcolor: 'grey.50' }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', width: '100%', alignItems: 'center' }}>
          <Typography variant="body2" color="text.secondary">
            {validEmails.length} participant(s) ready to add
          </Typography>
          
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Button onClick={handleClose} color="inherit">
              Cancel
            </Button>
            
            {currentTab < 2 ? (
              <Button
                variant="contained"
                onClick={() => setCurrentTab(currentTab + 1)}
                disabled={currentTab === 0 && validEmails.length === 0}
                endIcon={<ArrowForward />}
              >
                {currentTab === 0 ? `Next (${validEmails.length} emails)` : 'Next'}
              </Button>
            ) : (
              <Button
                variant="contained"
                onClick={handleAddParticipants}
                disabled={isProcessing || validEmails.length === 0}
                startIcon={isProcessing ? <CircularProgress size={20} /> : <Add />}
                sx={{
                  background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
                  minWidth: '160px'
                }}
              >
                {isProcessing ? 'Adding...' : `Add Participants (${validEmails.length})`}
              </Button>
            )}
          </Box>
        </Box>
      </DialogActions>
    </StyledDialog>
  );
}

export default BulkInvite;