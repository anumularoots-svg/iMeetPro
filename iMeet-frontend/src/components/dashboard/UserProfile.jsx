import React, { useState, useRef } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Avatar,
  Button,
  TextField,
  Grid,
  IconButton,
  Chip,
  Divider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  useTheme,
  alpha,
  Badge,
  Tooltip
} from '@mui/material';
import {
  Edit,
  Camera,
  Phone,
  Email,
  LocationOn,
  Language,
  Settings,
  Security,
  Notifications,
  VideoCall,
  Schedule,
  Star,
  TrendingUp,
  Save,
  Cancel
} from '@mui/icons-material';

const UserProfile = () => {
  const theme = useTheme();
  const fileInputRef = useRef(null);
  const [editMode, setEditMode] = useState(false);
  const [profileData, setProfileData] = useState({
    fullName: 'John Doe',
    email: 'john.doe@example.com',
    phone: '+1 (555) 123-4567',
    address: '123 Main Street, City, State',
    country: 'United States',
    languages: ['English', 'Spanish'],
    profilePicture: '/api/placeholder/120/120',
    status: 'Available',
    joinDate: '2024-01-15'
  });

  const [editedData, setEditedData] = useState(profileData);
  const [avatarDialog, setAvatarDialog] = useState(false);

  const countries = [
    'United States', 'Canada', 'United Kingdom', 'Germany', 'France', 
    'Spain', 'Italy', 'Australia', 'Japan', 'India', 'Brazil', 'Mexico'
  ];

  const languageOptions = [
    'English', 'Spanish', 'French', 'German', 'Italian', 'Portuguese',
    'Chinese', 'Japanese', 'Korean', 'Russian', 'Arabic', 'Hindi'
  ];

  const statusOptions = [
    { value: 'Available', color: '#4CAF50', label: 'Available' },
    { value: 'Busy', color: '#F44336', label: 'Busy' },
    { value: 'Away', color: '#FF9800', label: 'Away' },
    { value: 'Do Not Disturb', color: '#9C27B0', label: 'Do Not Disturb' }
  ];

  const meetingStats = [
    { label: 'Total Meetings', value: 127, icon: <VideoCall />, color: '#2196F3' },
    { label: 'Hours Hosted', value: 89, icon: <Schedule />, color: '#4CAF50' },
    { label: 'Average Rating', value: 4.8, icon: <Star />, color: '#FFD700' },
    { label: 'This Month', value: 24, icon: <TrendingUp />, color: '#FF9800' }
  ];

  const handleEdit = () => {
    setEditMode(true);
    setEditedData(profileData);
  };

  const handleSave = () => {
    setProfileData(editedData);
    setEditMode(false);
    // Here you would typically make an API call to update the profile
  };

  const handleCancel = () => {
    setEditedData(profileData);
    setEditMode(false);
  };

  const handleAvatarChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onloadend = () => {
        setEditedData({ ...editedData, profilePicture: reader.result });
        setAvatarDialog(false);
      };
      reader.readAsDataURL(file);
    }
  };

  const handleLanguageChange = (event) => {
    setEditedData({ ...editedData, languages: event.target.value });
  };

  const getStatusColor = (status) => {
    return statusOptions.find(s => s.value === status)?.color || '#757575';
  };

  return (
    <Box sx={{ p: 3 }}>
      {/* Header */}
      <Box sx={{ mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Box>
          <Typography 
            variant="h5" 
            sx={{ 
              fontWeight: 700,
              mb: 1,
              background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
              backgroundClip: 'text',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent'
            }}
          >
            User Profile
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Manage your account settings and preferences
          </Typography>
        </Box>
        {!editMode && (
          <Button
            variant="contained"
            startIcon={<Edit />}
            onClick={handleEdit}
            sx={{ borderRadius: 2 }}
          >
            Edit Profile
          </Button>
        )}
      </Box>

      <Grid container spacing={3}>
        {/* Profile Card */}
        <Grid item xs={12} md={4}>
          <Card sx={{ textAlign: 'center', p: 3 }}>
            <Badge
              overlap="circular"
              anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
              badgeContent={
                <Box
                  sx={{
                    width: 16,
                    height: 16,
                    borderRadius: '50%',
                    backgroundColor: getStatusColor(profileData.status),
                    border: `2px solid ${theme.palette.background.paper}`
                  }}
                />
              }
            >
              <Avatar
                src={editMode ? editedData.profilePicture : profileData.profilePicture}
                sx={{ 
                  width: 120, 
                  height: 120, 
                  mx: 'auto', 
                  mb: 2,
                  border: `4px solid ${alpha(theme.palette.primary.main, 0.1)}`
                }}
              />
            </Badge>
            {editMode && (
              <Box sx={{ mb: 2 }}>
                <Button
                  variant="outlined"
                  startIcon={<Camera />}
                  onClick={() => setAvatarDialog(true)}
                  size="small"
                >
                  Change Photo
                </Button>
              </Box>
            )}
            <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
              {editMode ? editedData.fullName : profileData.fullName}
            </Typography>
            <Chip
              label={profileData.status}
              sx={{
                backgroundColor: alpha(getStatusColor(profileData.status), 0.1),
                color: getStatusColor(profileData.status),
                fontWeight: 500
              }}
            />
            <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
              Member since {new Date(profileData.joinDate).toLocaleDateString('en-US', { 
                year: 'numeric', 
                month: 'long' 
              })}
            </Typography>
          </Card>

          {/* Quick Stats */}
          <Card sx={{ mt: 3 }}>
            <CardContent>
              <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                Meeting Statistics
              </Typography>
              {meetingStats.map((stat, index) => (
                <Box key={index} sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      width: 40,
                      height: 40,
                      borderRadius: '50%',
                      backgroundColor: alpha(stat.color, 0.1),
                      color: stat.color,
                      mr: 2
                    }}
                  >
                    {stat.icon}
                  </Box>
                  <Box sx={{ flexGrow: 1 }}>
                    <Typography variant="body2" color="text.secondary">
                      {stat.label}
                    </Typography>
                    <Typography variant="h6" sx={{ fontWeight: 600 }}>
                      {stat.value}
                    </Typography>
                  </Box>
                </Box>
              ))}
            </CardContent>
          </Card>
        </Grid>

        {/* Profile Details */}
        <Grid item xs={12} md={8}>
          <Card>
            <CardContent sx={{ p: 3 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  Personal Information
                </Typography>
                {editMode && (
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <Button
                      variant="outlined"
                      startIcon={<Cancel />}
                      onClick={handleCancel}
                      size="small"
                    >
                      Cancel
                    </Button>
                    <Button
                      variant="contained"
                      startIcon={<Save />}
                      onClick={handleSave}
                      size="small"
                    >
                      Save Changes
                    </Button>
                  </Box>
                )}
              </Box>

              <Grid container spacing={3}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Full Name"
                    value={editMode ? editedData.fullName : profileData.fullName}
                    onChange={(e) => setEditedData({ ...editedData, fullName: e.target.value })}
                    disabled={!editMode}
                    InputProps={{
                      startAdornment: <Edit sx={{ mr: 1, color: 'action.active' }} />
                    }}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Email"
                    value={editMode ? editedData.email : profileData.email}
                    onChange={(e) => setEditedData({ ...editedData, email: e.target.value })}
                    disabled={!editMode}
                    InputProps={{
                      startAdornment: <Email sx={{ mr: 1, color: 'action.active' }} />
                    }}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Phone Number"
                    value={editMode ? editedData.phone : profileData.phone}
                    onChange={(e) => setEditedData({ ...editedData, phone: e.target.value })}
                    disabled={!editMode}
                    InputProps={{
                      startAdornment: <Phone sx={{ mr: 1, color: 'action.active' }} />
                    }}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <FormControl fullWidth disabled={!editMode}>
                    <InputLabel>Country</InputLabel>
                    <Select
                      value={editMode ? editedData.country : profileData.country}
                      onChange={(e) => setEditedData({ ...editedData, country: e.target.value })}
                      startAdornment={<LocationOn sx={{ mr: 1, color: 'action.active' }} />}
                    >
                      {countries.map((country) => (
                        <MenuItem key={country} value={country}>
                          {country}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <TextField
                    fullWidth
                    label="Address"
                    value={editMode ? editedData.address : profileData.address}
                    onChange={(e) => setEditedData({ ...editedData, address: e.target.value })}
                    disabled={!editMode}
                    multiline
                    rows={2}
                    InputProps={{
                      startAdornment: <LocationOn sx={{ mr: 1, color: 'action.active', alignSelf: 'flex-start', mt: 1 }} />
                    }}
                  />
                </Grid>
                <Grid item xs={12}>
                  <FormControl fullWidth disabled={!editMode}>
                    <InputLabel>Languages</InputLabel>
                    <Select
                      multiple
                      value={editMode ? editedData.languages : profileData.languages}
                      onChange={handleLanguageChange}
                      startAdornment={<Language sx={{ mr: 1, color: 'action.active' }} />}
                      renderValue={(selected) => (
                        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                          {selected.map((value) => (
                            <Chip key={value} label={value} size="small" />
                          ))}
                        </Box>
                      )}
                    >
                      {languageOptions.map((language) => (
                        <MenuItem key={language} value={language}>
                          {language}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                {editMode && (
                  <Grid item xs={12}>
                    <FormControl fullWidth>
                      <InputLabel>Status</InputLabel>
                      <Select
                        value={editedData.status}
                        onChange={(e) => setEditedData({ ...editedData, status: e.target.value })}
                      >
                        {statusOptions.map((status) => (
                          <MenuItem key={status.value} value={status.value}>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Box
                                sx={{
                                  width: 12,
                                  height: 12,
                                  borderRadius: '50%',
                                  backgroundColor: status.color
                                }}
                              />
                              {status.label}
                            </Box>
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  </Grid>
                )}
              </Grid>
            </CardContent>
          </Card>

          {/* Additional Settings */}
          <Card sx={{ mt: 3 }}>
            <CardContent>
              <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                Account Settings
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} sm={4}>
                  <Button
                    fullWidth
                    variant="outlined"
                    startIcon={<Security />}
                    sx={{ py: 1.5 }}
                  >
                    Security
                  </Button>
                </Grid>
                <Grid item xs={12} sm={4}>
                  <Button
                    fullWidth
                    variant="outlined"
                    startIcon={<Notifications />}
                    sx={{ py: 1.5 }}
                  >
                    Notifications
                  </Button>
                </Grid>
                <Grid item xs={12} sm={4}>
                  <Button
                    fullWidth
                    variant="outlined"
                    startIcon={<Settings />}
                    sx={{ py: 1.5 }}
                  >
                    Preferences
                  </Button>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Avatar Change Dialog */}
      <Dialog open={avatarDialog} onClose={() => setAvatarDialog(false)}>
        <DialogTitle>Change Profile Picture</DialogTitle>
        <DialogContent>
          <Box sx={{ textAlign: 'center', py: 2 }}>
            <input
              ref={fileInputRef}
              type="file"
              accept="image/*"
              onChange={handleAvatarChange}
              style={{ display: 'none' }}
            />
            <Button
              variant="contained"
              startIcon={<Camera />}
              onClick={() => fileInputRef.current?.click()}
              sx={{ mb: 2 }}
            >
              Choose Photo
            </Button>
            <Typography variant="body2" color="text.secondary">
              Supported formats: JPG, PNG, GIF (max 5MB)
            </Typography>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAvatarDialog(false)}>Cancel</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default UserProfile;