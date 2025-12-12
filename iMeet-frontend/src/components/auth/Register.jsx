// src/components/auth/Register.jsx - COMPLETE VERSION WITH PHOTO
import React, { useState, useRef } from 'react';
import {
  Box,
  Container,
  Grid,
  TextField,
  Button,
  Typography,
  Link,
  Alert,
  InputAdornment,
  IconButton,
  MenuItem,
  FormControl,
  InputLabel,
  Select,
  Checkbox,
  FormControlLabel,
  CircularProgress,
  useTheme,
  alpha,
  Stepper,
  Step,
  StepLabel,
  StepConnector,
  Chip,
  Stack,
  styled,
  Paper
} from '@mui/material';
import {
  Visibility,
  VisibilityOff,
  Email,
  Lock,
  Person,
  Phone,
  LocationOn,
  Language,
  PersonAdd,
  ArrowBack,
  ArrowForward,
  Check,
  VideoCall,
  Security,
  Verified,
  CameraAlt,
  Image
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../hooks/useAuth';
import CameraCapture from './CameraCapture';

const countries = [
  { code: '+1', name: 'United States', flag: '🇺🇸' },
  { code: '+91', name: 'India', flag: '🇮🇳' },
  { code: '+44', name: 'United Kingdom', flag: '🇬🇧' },
  { code: '+86', name: 'China', flag: '🇨🇳' },
  { code: '+49', name: 'Germany', flag: '🇩🇪' },
  { code: '+33', name: 'France', flag: '🇫🇷' },
  { code: '+81', name: 'Japan', flag: '🇯🇵' },
  { code: '+7', name: 'Russia', flag: '🇷🇺' },
  { code: '+39', name: 'Italy', flag: '🇮🇹' },
  { code: '+34', name: 'Spain', flag: '🇪🇸' }
];

const languages = [
  'English', 'Spanish', 'French', 'German', 'Chinese', 'Japanese', 
  'Korean', 'Portuguese', 'Russian', 'Arabic', 'Hindi', 'Italian'
];

const steps = ['Personal Info', 'Contact Details', 'Account Setup'];

// Custom Stepper Connector
const CustomStepConnector = styled(StepConnector)(({ theme }) => ({
  '&.Mui-active': {
    '& .MuiStepConnector-line': {
      background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
    },
  },
  '&.Mui-completed': {
    '& .MuiStepConnector-line': {
      background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
    },
  },
  '& .MuiStepConnector-line': {
    height: 3,
    border: 0,
    backgroundColor: theme.palette.grey[300],
    borderRadius: 1,
  },
}));

// Custom Step Icon
const CustomStepIcon = styled('div')(({ theme, ownerState }) => ({
  backgroundColor: ownerState.completed || ownerState.active 
    ? 'transparent' 
    : theme.palette.grey[300],
  background: ownerState.completed || ownerState.active 
    ? `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`
    : theme.palette.grey[300],
  zIndex: 1,
  color: '#fff',
  width: 40,
  height: 40,
  display: 'flex',
  borderRadius: '50%',
  justifyContent: 'center',
  alignItems: 'center',
  boxShadow: ownerState.completed || ownerState.active 
    ? '0 8px 24px rgba(0,0,0,0.15)'
    : 'none',
  transition: 'all 0.3s ease',
  fontSize: '1rem',
  fontWeight: 'bold',
}));

const StyledTextField = styled(TextField)(({ theme }) => ({
  '& .MuiOutlinedInput-root': {
    borderRadius: 12,
    backgroundColor: alpha(theme.palette.background.paper, 0.9),
    transition: 'all 0.3s ease',
    '&:hover': {
      backgroundColor: theme.palette.background.paper,
      '& .MuiOutlinedInput-notchedOutline': {
        borderColor: alpha(theme.palette.primary.main, 0.5),
      },
    },
    '&.Mui-focused': {
      backgroundColor: theme.palette.background.paper,
      boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.1)}`,
      '& .MuiOutlinedInput-notchedOutline': {
        borderColor: theme.palette.primary.main,
        borderWidth: 2,
      },
    },
  },
  '& .MuiInputLabel-root': {
    fontWeight: 500,
  },
}));

const Register = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const { register, loading } = useAuth();
  
  const [activeStep, setActiveStep] = useState(0);
  const [formData, setFormData] = useState({
    full_name: '',
    email: '',
    phone_number: '',
    country_code: '+91',
    address: '',
    country: '',
    languages: [],
    password: '',
    confirmPassword: '',
    agreeToTerms: false
  });
  
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [errors, setErrors] = useState({});
  const [apiError, setApiError] = useState('');
  const [languageMenuOpen, setLanguageMenuOpen] = useState(false);
  
  // Photo capture states
  const [cameraOpen, setCameraOpen] = useState(false);
  const [profilePhoto, setProfilePhoto] = useState(null);
  const profilePhotoRef = useRef(null); // Backup ref for photo

  const handleChange = (e) => {
    const { name, value, checked, type } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value
    }));
    
    if (errors[name]) {
      setErrors(prev => ({ ...prev, [name]: '' }));
    }
  };

  const handleLanguageChange = (event) => {
    const value = event.target.value;
    setFormData(prev => ({
      ...prev,
      languages: typeof value === 'string' ? value.split(',') : value
    }));
  };

  const handlePhotoCapture = (photoData) => {
    console.log('📸 Photo captured in Register.jsx');
    console.log('📸 Photo data length:', photoData?.length);
    console.log('📸 Photo preview:', photoData?.substring(0, 100));
    
    // Store in both state and ref
    setProfilePhoto(photoData);
    profilePhotoRef.current = photoData;
    
    // Clear photo error if exists
    if (errors.profilePhoto) {
      setErrors(prev => ({ ...prev, profilePhoto: '' }));
    }
    
    console.log('📸 Photo state updated successfully');
  };

  const validateStep = (step) => {
    const newErrors = {};
    
    switch (step) {
      case 0: // Personal Info
        if (!formData.full_name.trim()) {
          newErrors.full_name = 'Full name is required';
        } else if (formData.full_name.trim().length < 2) {
          newErrors.full_name = 'Full name must be at least 2 characters';
        }
        
        if (!formData.email) {
          newErrors.email = 'Email is required';
        } else if (!/\S+@\S+\.\S+/.test(formData.email)) {
          newErrors.email = 'Please enter a valid email address';
        }
        
        // Check both state and ref for photo
        const photo = profilePhoto || profilePhotoRef.current;
        if (!photo) {
          newErrors.profilePhoto = 'Profile photo is required';
        }
        break;
        
      case 1: // Contact Details
        if (!formData.phone_number) {
          newErrors.phone_number = 'Phone number is required';
        } else if (!/^\d{10,15}$/.test(formData.phone_number.replace(/\s|-/g, ''))) {
          newErrors.phone_number = 'Please enter a valid phone number';
        }
        
        if (!formData.address.trim()) {
          newErrors.address = 'Address is required';
        }
        
        if (!formData.country) {
          newErrors.country = 'Country is required';
        }
        
        if (formData.languages.length === 0) {
          newErrors.languages = 'Please select at least one language';
        }
        break;
        
      case 2: // Account Setup
        if (!formData.password) {
          newErrors.password = 'Password is required';
        } else if (formData.password.length < 8) {
          newErrors.password = 'Password must be at least 8 characters';
        } else if (!/(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/.test(formData.password)) {
          newErrors.password = 'Password must contain uppercase, lowercase, and number';
        }
        
        if (formData.password !== formData.confirmPassword) {
          newErrors.confirmPassword = 'Passwords do not match';
        }
        
        if (!formData.agreeToTerms) {
          newErrors.agreeToTerms = 'You must agree to the terms and conditions';
        }
        break;
    }
    
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleNext = () => {
    if (validateStep(activeStep)) {
      setActiveStep(prev => prev + 1);
    }
  };

  const handleBack = () => {
    setActiveStep(prev => prev - 1);
  };

  const handleSubmit = async (e) => {
  e.preventDefault();
  setApiError('');

  console.log('🚀 Submit button clicked');
  
  if (!validateStep(2)) {
    console.log('❌ Step 2 validation failed');
    return;
  }
  
  // Get photo from state or ref
  const photo = profilePhoto || profilePhotoRef.current;
  
  console.log('🚀 Photo check:');
  console.log('- Photo from state:', !!profilePhoto);
  console.log('- Photo from ref:', !!profilePhotoRef.current);
  console.log('- Using photo:', !!photo);
  console.log('- Photo length:', photo?.length);
  
  if (!photo) {
    console.error('❌ No profile photo found!');
    setApiError('Please capture a profile photo before submitting');
    setActiveStep(0); // Go back to step 1
    return;
  }
  
  // Validate photo format
  if (!photo.startsWith('data:image')) {
    console.error('❌ Invalid photo format');
    setApiError('Invalid photo format. Please capture the photo again.');
    setActiveStep(0);
    return;
  }
  
  try {
    const registrationData = {
      full_name: formData.full_name,
      email: formData.email,
      phone_number: formData.phone_number,
      password: formData.password,
      address: formData.address,
      country: formData.country,
      country_code: formData.country_code,
      languages: formData.languages.join(','),
      profile_photo: photo,
      agreeToTerms: formData.agreeToTerms
    };
    
    console.log('📤 Sending registration data:');
    console.log('- Full Name:', registrationData.full_name);
    console.log('- Email:', registrationData.email);
    console.log('- Has Photo:', !!registrationData.profile_photo);
    console.log('- Photo Length:', registrationData.profile_photo?.length);
    console.log('- Photo Preview:', registrationData.profile_photo?.substring(0, 50));
    console.log('- Data Keys:', Object.keys(registrationData));
    
    console.log('✅ Calling register function...');
    const result = await register(registrationData);
    
    console.log('✅ Registration successful!', result);
    
    // ✅ CHANGE THIS LINE - Navigate to login with success state
    navigate('/auth/login', { 
      state: { 
        registrationSuccess: true,
        email: formData.email 
      } 
    });
  } catch (error) {
    console.error('❌ Registration failed:', error);
    setApiError(error.message || 'Registration failed. Please try again.');
  }
};
  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Stack spacing={3}>
            <StyledTextField
              fullWidth
              name="full_name"
              label="Full Name"
              value={formData.full_name}
              onChange={handleChange}
              error={!!errors.full_name}
              helperText={errors.full_name}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <Person color="action" />
                  </InputAdornment>
                ),
              }}
            />

            <StyledTextField
              fullWidth
              name="email"
              label="Email Address"
              type="email"
              value={formData.email}
              onChange={handleChange}
              error={!!errors.email}
              helperText={errors.email}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <Email color="action" />
                  </InputAdornment>
                ),
              }}
            />

            {/* Profile Photo Capture Section */}
            <Box sx={{ textAlign: 'center', my: 3 }}>
              <Typography variant="subtitle2" color="text.secondary" mb={2} fontWeight={600}>
                Profile Photo *
              </Typography>
              
              <Box
                sx={{
                  width: 150,
                  height: 150,
                  mx: 'auto',
                  mb: 2,
                  borderRadius: '50%',
                  border: `3px dashed ${errors.profilePhoto ? theme.palette.error.main : alpha(theme.palette.primary.main, 0.3)}`,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  overflow: 'hidden',
                  backgroundColor: alpha(theme.palette.background.default, 0.5),
                  position: 'relative',
                  cursor: 'pointer',
                  transition: 'all 0.3s ease',
                  '&:hover': {
                    borderColor: errors.profilePhoto ? theme.palette.error.main : theme.palette.primary.main,
                    backgroundColor: alpha(theme.palette.primary.main, 0.05),
                    transform: 'scale(1.02)',
                  }
                }}
                onClick={() => setCameraOpen(true)}
              >
                {(profilePhoto || profilePhotoRef.current) ? (
                  <>
                    <img
                      src={profilePhoto || profilePhotoRef.current}
                      alt="Profile"
                      style={{
                        width: '100%',
                        height: '100%',
                        objectFit: 'cover'
                      }}
                    />
                    <Box
                      sx={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        right: 0,
                        bottom: 0,
                        backgroundColor: 'rgba(0,0,0,0)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        transition: 'all 0.3s ease',
                        '&:hover': {
                          backgroundColor: 'rgba(0,0,0,0.5)',
                        }
                      }}
                    >
                      <Image 
                        sx={{ 
                          color: 'white', 
                          fontSize: 40,
                          opacity: 0,
                          transition: 'opacity 0.3s ease',
                          '&:hover': {
                            opacity: 1
                          }
                        }} 
                      />
                    </Box>
                  </>
                ) : (
                  <Stack alignItems="center" spacing={1}>
                    <CameraAlt sx={{ fontSize: 40, color: 'text.secondary' }} />
                    <Typography variant="caption" color="text.secondary">
                      Add Photo
                    </Typography>
                  </Stack>
                )}
              </Box>
              
              <Button
                variant="outlined"
                startIcon={(profilePhoto || profilePhotoRef.current) ? <Image /> : <CameraAlt />}
                onClick={() => setCameraOpen(true)}
                sx={{
                  borderRadius: 2,
                  textTransform: 'none',
                  px: 3,
                  py: 1,
                  borderColor: errors.profilePhoto ? theme.palette.error.main : alpha(theme.palette.primary.main, 0.3),
                  color: errors.profilePhoto ? theme.palette.error.main : theme.palette.primary.main,
                  '&:hover': {
                    borderColor: errors.profilePhoto ? theme.palette.error.dark : theme.palette.primary.main,
                    backgroundColor: alpha(theme.palette.primary.main, 0.05)
                  }
                }}
              >
                {(profilePhoto || profilePhotoRef.current) ? 'Change Photo' : 'Capture Photo'}
              </Button>
              
              {errors.profilePhoto && (
                <Typography variant="caption" color="error" display="block" sx={{ mt: 1 }}>
                  {errors.profilePhoto}
                </Typography>
              )}
              
              {(profilePhoto || profilePhotoRef.current) && !errors.profilePhoto && (
                <Typography variant="caption" color="success.main" display="block" sx={{ mt: 1, fontWeight: 600 }}>
                  ✓ Photo captured successfully
                </Typography>
              )}
            </Box>
          </Stack>
        );
        
      case 1:
        return (
          <Stack spacing={3}>
            <Grid container spacing={2}>
              <Grid item xs={4}>
                <FormControl fullWidth>
                  <InputLabel>Country Code</InputLabel>
                  <Select
                    name="country_code"
                    value={formData.country_code}
                    onChange={handleChange}
                    label="Country Code"
                    sx={{ borderRadius: 3 }}
                  >
                    {countries.map((country) => (
                      <MenuItem key={country.code} value={country.code}>
                        <Box display="flex" alignItems="center" gap={1}>
                          <span>{country.flag}</span>
                          <span>{country.code}</span>
                        </Box>
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={8}>
                <StyledTextField
                  fullWidth
                  name="phone_number"
                  label="Phone Number"
                  value={formData.phone_number}
                  onChange={handleChange}
                  error={!!errors.phone_number}
                  helperText={errors.phone_number}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <Phone color="action" />
                      </InputAdornment>
                    ),
                  }}
                />
              </Grid>
            </Grid>

            <StyledTextField
              fullWidth
              name="address"
              label="Address"
              multiline
              rows={2}
              value={formData.address}
              onChange={handleChange}
              error={!!errors.address}
              helperText={errors.address}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <LocationOn color="action" />
                  </InputAdornment>
                ),
              }}
            />

            <StyledTextField
              fullWidth
              name="country"
              label="Country"
              value={formData.country}
              onChange={handleChange}
              error={!!errors.country}
              helperText={errors.country}
            />

            <FormControl fullWidth error={!!errors.languages}>
              <InputLabel>Languages</InputLabel>
              <Select
                multiple
                name="languages"
                value={formData.languages}
                onChange={handleLanguageChange}
                onClose={() => setLanguageMenuOpen(false)}
                onOpen={() => setLanguageMenuOpen(true)}
                open={languageMenuOpen}
                label="Languages"
                sx={{ borderRadius: 3 }}
                startAdornment={
                  <InputAdornment position="start">
                    <Language color="action" />
                  </InputAdornment>
                }
                renderValue={(selected) => (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {selected.map((value) => (
                      <Chip key={value} label={value} size="small" color="primary" variant="outlined" />
                    ))}
                  </Box>
                )}
              >
                {languages.map((language) => (
                  <MenuItem 
                    key={language} 
                    value={language}
                    onClick={() => {
                      setTimeout(() => setLanguageMenuOpen(false), 100);
                    }}
                  >
                    <Checkbox checked={formData.languages.indexOf(language) > -1} />
                    {language}
                  </MenuItem>
                ))}
              </Select>
              {errors.languages && (
                <Typography variant="caption" color="error" sx={{ mt: 0.5, ml: 2 }}>
                  {errors.languages}
                </Typography>
              )}
            </FormControl>
          </Stack>
        );
        
      case 2:
        return (
          <Stack spacing={3}>
            <StyledTextField
              fullWidth
              name="password"
              label="Password"
              type={showPassword ? 'text' : 'password'}
              value={formData.password}
              onChange={handleChange}
              error={!!errors.password}
              helperText={errors.password || 'Must contain uppercase, lowercase, and number'}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <Lock color="action" />
                  </InputAdornment>
                ),
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton
                      onClick={() => setShowPassword(!showPassword)}
                      edge="end"
                    >
                      {showPassword ? <VisibilityOff /> : <Visibility />}
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />

            <StyledTextField
              fullWidth
              name="confirmPassword"
              label="Confirm Password"
              type={showConfirmPassword ? 'text' : 'password'}
              value={formData.confirmPassword}
              onChange={handleChange}
              error={!!errors.confirmPassword}
              helperText={errors.confirmPassword}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <Lock color="action" />
                  </InputAdornment>
                ),
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton
                      onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                      edge="end"
                    >
                      {showConfirmPassword ? <VisibilityOff /> : <Visibility />}
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />

            <Paper 
              elevation={0} 
              sx={{ 
                p: 3, 
                backgroundColor: alpha(theme.palette.background.default, 0.5),
                borderRadius: 3,
                border: errors.agreeToTerms ? `2px solid ${theme.palette.error.main}` : `1px solid ${alpha(theme.palette.primary.main, 0.1)}`,
              }}
            >
              <FormControlLabel
                control={
                  <Checkbox
                    name="agreeToTerms"
                    checked={formData.agreeToTerms}
                    onChange={handleChange}
                    color="primary"
                  />
                }
                label={
                  <Typography variant="body2" sx={{ lineHeight: 1.6 }}>
                    I agree to the{' '}
                    <Link href="#" color="primary" sx={{ fontWeight: 600 }}>
                      Terms of Service
                    </Link>
                    {' '}and{' '}
                    <Link href="#" color="primary" sx={{ fontWeight: 600 }}>
                      Privacy Policy
                    </Link>
                  </Typography>
                }
              />
            </Paper>
            
            {errors.agreeToTerms && (
              <Typography variant="caption" color="error" display="block" sx={{ ml: 1 }}>
                {errors.agreeToTerms}
              </Typography>
            )}
          </Stack>
        );
        
      default:
        return null;
    }
  };

  return (
    <Box
      sx={{
        minHeight: '100vh',
        width: '100vw',
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
        '&::before': {
          content: '""',
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.03"%3E%3Ccircle cx="30" cy="30" r="1.5"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
        },
        overflow: 'auto'
      }}
    >
      <Grid container sx={{ height: '100vh' }}>
        {/* Left Side - Branding */}
        <Grid item xs={12} md={6} lg={7}>
          <Box
            sx={{
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              px: { xs: 3, md: 6, lg: 8 },
              color: 'white',
              position: 'relative',
              zIndex: 1
            }}
          >
            {/* Logo */}
            <Stack direction="row" alignItems="center" spacing={2} mb={4}>
              <Box
                sx={{
                  width: 60,
                  height: 60,
                  borderRadius: 2,
                  background: alpha(theme.palette.background.paper, 0.15),
                  backdropFilter: 'blur(20px)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  border: `2px solid ${alpha(theme.palette.common.white, 0.2)}`,
                }}
              >
                <VideoCall sx={{ fontSize: 32, color: 'white' }} />
              </Box>
              <Typography variant="h3" fontWeight={700}>
                iMeetPro
              </Typography>
            </Stack>

            {/* Welcome Message */}
            <Typography variant="h2" fontWeight={700} mb={3} sx={{ lineHeight: 1.2 }}>
              Join the Future of Professional Communication
            </Typography>
            
            <Typography variant="h6" sx={{ opacity: 0.9, mb: 4, maxWidth: 600 }}>
              Create your account and unlock enterprise-grade video conferencing 
              with advanced collaboration features.
            </Typography>

            {/* Benefits */}
            <Stack spacing={2} mb={4}>
              {[
                { icon: <Security />, text: 'Bank-grade security & encryption' },
                { icon: <Verified />, text: 'SOC 2 Type II certified platform' },
                { icon: <PersonAdd />, text: 'Unlimited team members' },
                { icon: <VideoCall />, text: 'HD video with AI enhancements' }
              ].map((benefit, index) => (
                <Stack key={index} direction="row" alignItems="center" spacing={2}>
                  {React.cloneElement(benefit.icon, { 
                    sx: { color: alpha(theme.palette.common.white, 0.8) } 
                  })}
                  <Typography variant="body1" sx={{ opacity: 0.9 }}>
                    {benefit.text}
                  </Typography>
                </Stack>
              ))}
            </Stack>

            {/* Trusted By */}
            <Typography variant="h6" fontWeight={600} mb={2}>
              Trusted by 10M+ professionals
            </Typography>
            <Typography variant="body2" sx={{ opacity: 0.8 }}>
              From startups to Fortune 500 companies
            </Typography>
          </Box>
        </Grid>

        {/* Right Side - Registration Form */}
        <Grid item xs={12} md={6} lg={5}>
          <Box
            sx={{
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              px: { xs: 3, md: 4, lg: 5 },
              py: { xs: 4, md: 6 },
              background: alpha(theme.palette.background.paper, 0.95),
              backdropFilter: 'blur(20px)',
              position: 'relative',
              zIndex: 1,
              borderLeft: { md: `1px solid ${alpha(theme.palette.common.white, 0.1)}` },
              overflow: 'auto'
            }}
          >
            {/* Header */}
            <Box textAlign="center" mb={4}>
              <Typography 
                variant="h4" 
                fontWeight={700} 
                gutterBottom
                sx={{
                  background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                  backgroundClip: 'text',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent',
                }}
              >
                Create Account
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Join our platform and start your professional journey
              </Typography>
            </Box>

            {/* Enhanced Stepper */}
            <Box mb={4}>
              <Stepper 
                activeStep={activeStep} 
                connector={<CustomStepConnector />}
                sx={{ mb: 3 }}
              >
                {steps.map((label, index) => (
                  <Step key={label}>
                    <StepLabel
                      StepIconComponent={({ active, completed }) => (
                        <CustomStepIcon ownerState={{ active, completed }}>
                          {completed ? <Check /> : index + 1}
                        </CustomStepIcon>
                      )}
                      sx={{
                        '& .MuiStepLabel-label': {
                          fontSize: '0.9rem',
                          fontWeight: 600,
                          mt: 1
                        }
                      }}
                    >
                      {label}
                    </StepLabel>
                  </Step>
                ))}
              </Stepper>
            </Box>

            {/* Error Alert */}
            {apiError && (
              <Alert 
                severity="error" 
                sx={{ 
                  mb: 3, 
                  borderRadius: 2,
                  '& .MuiAlert-message': {
                    fontWeight: 500
                  }
                }}
                onClose={() => setApiError('')}
              >
                {apiError}
              </Alert>
            )}

            {/* Form Content */}
            <Box component="form" onSubmit={handleSubmit}>
              <Box sx={{ minHeight: 280, mb: 4 }}>
                {renderStepContent(activeStep)}
              </Box>

              {/* Navigation Buttons */}
              <Stack direction="row" justifyContent="space-between" spacing={2} mb={3}>
                {activeStep === 0 ? (
                  <Button
                    variant="outlined"
                    startIcon={<ArrowBack />}
                    onClick={() => navigate('/login')}
                    sx={{ 
                      textTransform: 'none',
                      borderRadius: 2,
                      px: 3,
                      py: 1.5,
                      borderColor: alpha(theme.palette.primary.main, 0.3),
                      '&:hover': {
                        borderColor: theme.palette.primary.main,
                        backgroundColor: alpha(theme.palette.primary.main, 0.05)
                      }
                    }}
                  >
                    Back to Login
                  </Button>
                ) : (
                  <Button
                    variant="outlined"
                    startIcon={<ArrowBack />}
                    onClick={handleBack}
                    sx={{ 
                      textTransform: 'none',
                      borderRadius: 2,
                      px: 3,
                      py: 1.5,
                      borderColor: alpha(theme.palette.primary.main, 0.3),
                      '&:hover': {
                        borderColor: theme.palette.primary.main,
                        backgroundColor: alpha(theme.palette.primary.main, 0.05)
                      }
                    }}
                  >
                    Previous
                  </Button>
                )}

                {activeStep === steps.length - 1 ? (
                  <Button
                    type="submit"
                    variant="contained"
                    disabled={loading}
                    startIcon={loading ? <CircularProgress size={20} /> : <PersonAdd />}
                    sx={{
                      textTransform: 'none',
                      px: 4,
                      py: 1.5,
                      borderRadius: 2,
                      background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                      boxShadow: '0 8px 24px rgba(0, 0, 0, 0.15)',
                      fontSize: '1rem',
                      fontWeight: 600,
                      '&:hover': {
                        boxShadow: '0 12px 32px rgba(0, 0, 0, 0.25)',
                        transform: 'translateY(-2px)'
                      },
                      '&:disabled': {
                        background: theme.palette.grey[400]
                      }
                    }}
                  >
                    {loading ? 'Creating Account...' : 'Create Account'}
                  </Button>
                ) : (
                  <Button
                    variant="contained"
                    endIcon={<ArrowForward />}
                    onClick={handleNext}
                    sx={{
                      textTransform: 'none',
                      px: 4,
                      py: 1.5,
                      borderRadius: 2,
                      background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                      boxShadow: '0 8px 24px rgba(0, 0, 0, 0.15)',
                      fontSize: '1rem',
                      fontWeight: 600,
                      '&:hover': {
                        boxShadow: '0 12px 32px rgba(0, 0, 0, 0.25)',
                        transform: 'translateY(-2px)'
                      }
                    }}
                  >
                    Continue
                  </Button>
                )}
              </Stack>

              {/* Sign In Link */}
              <Box textAlign="center">
                <Typography variant="body2" color="text.secondary">
                  Already have an account?{' '}
                  <Link
                    component="button"
                    type="button"
                    variant="body2"
                    onClick={() => navigate('/login')}
                    sx={{
                      textDecoration: 'none',
                      fontWeight: 600,
                      color: theme.palette.primary.main,
                      '&:hover': {
                        textDecoration: 'underline'
                      }
                    }}
                  >
                    Sign in here
                  </Link>
                </Typography>
              </Box>
            </Box>
          </Box>
        </Grid>
      </Grid>

      {/* Camera Capture Modal */}
      <CameraCapture
        open={cameraOpen}
        onClose={() => setCameraOpen(false)}
        onCapture={handlePhotoCapture}
        currentPhoto={profilePhoto || profilePhotoRef.current}
      />
    </Box>
  );
};

export default Register;