// // src/components/auth/ForgotPassword.jsx
// import React, { useState } from 'react';
// import {
//   Box,
//   Card,
//   CardContent,
//   TextField,
//   Button,
//   Typography,
//   Alert,
//   InputAdornment,
//   CircularProgress,
//   useTheme,
//   Stepper,
//   Step,
//   StepLabel,
//   Paper,
//   IconButton
// } from '@mui/material';
// import {
//   Email,
//   Lock,
//   LockReset,
//   ArrowBack,
//   Send,
//   Visibility,
//   VisibilityOff,
//   Security
// } from '@mui/icons-material';
// import { useNavigate } from 'react-router-dom';
// import { useAuth } from '../../hooks/useAuth';

// const ForgotPassword = () => {
//   const theme = useTheme();
//   const navigate = useNavigate();
//   // FIXED: Only need forgotPassword and resetPassword functions
//   const { forgotPassword, resetPassword, loading } = useAuth();
  
//   const [activeStep, setActiveStep] = useState(0);
//   const [formData, setFormData] = useState({
//     email: '',
//     otp: '',
//     newPassword: '',
//     confirmPassword: ''
//   });
  
//   const [showPassword, setShowPassword] = useState(false);
//   const [showConfirmPassword, setShowConfirmPassword] = useState(false);
//   const [errors, setErrors] = useState({});
//   const [apiError, setApiError] = useState('');
//   const [successMessage, setSuccessMessage] = useState('');

//   const steps = ['Enter Email', 'Reset Password'];

//   const handleChange = (e) => {
//     const { name, value } = e.target;
//     setFormData(prev => ({
//       ...prev,
//       [name]: value
//     }));
    
//     // Clear field error when user starts typing
//     if (errors[name]) {
//       setErrors(prev => ({ ...prev, [name]: '' }));
//     }
//   };

//   const validateEmail = () => {
//     const newErrors = {};
    
//     if (!formData.email) {
//       newErrors.email = 'Email is required';
//     } else if (!/\S+@\S+\.\S+/.test(formData.email)) {
//       newErrors.email = 'Please enter a valid email address';
//     }
    
//     setErrors(newErrors);
//     return Object.keys(newErrors).length === 0;
//   };

//   const validateResetForm = () => {
//     const newErrors = {};
    
//     // Validate OTP
//     if (!formData.otp) {
//       newErrors.otp = 'OTP is required';
//     } else if (formData.otp.length !== 6) {
//       newErrors.otp = 'OTP must be 6 digits';
//     }
    
//     // Validate Password
//     if (!formData.newPassword) {
//       newErrors.newPassword = 'New password is required';
//     } else if (formData.newPassword.length < 8) {
//       newErrors.newPassword = 'Password must be at least 8 characters';
//     } else if (!/(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/.test(formData.newPassword)) {
//       newErrors.newPassword = 'Password must contain uppercase, lowercase, and number';
//     }
    
//     // Validate Confirm Password
//     if (formData.newPassword !== formData.confirmPassword) {
//       newErrors.confirmPassword = 'Passwords do not match';
//     }
    
//     setErrors(newErrors);
//     return Object.keys(newErrors).length === 0;
//   };

//   const handleSendResetEmail = async (e) => {
//     e.preventDefault();
//     setApiError('');
    
//     if (!validateEmail()) return;
    
//     try {
//       console.log('Sending forgot password request for:', formData.email);
//       await forgotPassword(formData.email);
//       setSuccessMessage('Reset code sent to your email address');
//       setActiveStep(1);
//     } catch (error) {
//       console.error('Forgot password error:', error);
//       setApiError(error.message || 'Failed to send reset email. Please try again.');
//     }
//   };

//   const handleResetPassword = async (e) => {
//     e.preventDefault();
//     setApiError('');
    
//     if (!validateResetForm()) return;
    
//     try {
//       console.log('Sending reset password request with OTP:', formData.otp);
//       // FIXED: Send data in format expected by your backend
//       await resetPassword({
//         otp: formData.otp,              // Backend expects 'OTP'
//         password: formData.newPassword,  // Backend expects 'password'
//         email: formData.email
//       });
//       setSuccessMessage('Password reset successfully! You can now sign in with your new password.');
//       setTimeout(() => navigate('/login'), 3000);
//     } catch (error) {
//       console.error('Reset password error:', error);
//       setApiError(error.message || 'Failed to reset password. Please try again.');
//     }
//   };

//   const renderStepContent = (step) => {
//     switch (step) {
//       case 0:
//         return (
//           <Box component="form" onSubmit={handleSendResetEmail}>
//             <Typography variant="body1" color="text.secondary" mb={3} textAlign="center">
//               Enter your email address and we'll send you a 6-digit code to reset your password.
//             </Typography>
            
//             <TextField
//               fullWidth
//               name="email"
//               label="Email Address"
//               type="email"
//               value={formData.email}
//               onChange={handleChange}
//               error={!!errors.email}
//               helperText={errors.email}
//               InputProps={{
//                 startAdornment: (
//                   <InputAdornment position="start">
//                     <Email color="action" />
//                   </InputAdornment>
//                 ),
//               }}
//               sx={{ mb: 3 }}
//               variant="outlined"
//               autoFocus
//             />

//             <Button
//               type="submit"
//               fullWidth
//               variant="contained"
//               size="large"
//               disabled={loading}
//               startIcon={loading ? <CircularProgress size={20} /> : <Send />}
//               sx={{
//                 py: 1.5,
//                 borderRadius: 2,
//                 textTransform: 'none',
//                 fontSize: '1.1rem',
//                 fontWeight: 600,
//                 background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
//                 '&:hover': {
//                   background: `linear-gradient(135deg, ${theme.palette.primary.dark}, ${theme.palette.secondary.dark})`,
//                 }
//               }}
//             >
//               {loading ? 'Sending...' : 'Send Reset Code'}
//             </Button>
//           </Box>
//         );
        
//       case 1:
//         return (
//           <Box component="form" onSubmit={handleResetPassword}>
//             <Typography variant="body1" color="text.secondary" mb={3} textAlign="center">
//               Enter the 6-digit code sent to{' '}
//               <Typography component="span" fontWeight="bold" color="primary">
//                 {formData.email}
//               </Typography>
//               {' '}and create your new password.
//             </Typography>
            
//             {/* OTP Input */}
//             <TextField
//               fullWidth
//               name="otp"
//               label="Enter 6-digit OTP"
//               value={formData.otp}
//               onChange={handleChange}
//               error={!!errors.otp}
//               helperText={errors.otp}
//               InputProps={{
//                 startAdornment: (
//                   <InputAdornment position="start">
//                     <Security color="action" />
//                   </InputAdornment>
//                 ),
//               }}
//               inputProps={{
//                 maxLength: 6,
//                 style: { textAlign: 'center', fontSize: '1.2rem', letterSpacing: '0.3rem' }
//               }}
//               sx={{ mb: 3 }}
//               variant="outlined"
//               autoFocus
//             />

//             {/* New Password */}
//             <TextField
//               fullWidth
//               name="newPassword"
//               label="New Password"
//               type={showPassword ? 'text' : 'password'}
//               value={formData.newPassword}
//               onChange={handleChange}
//               error={!!errors.newPassword}
//               helperText={errors.newPassword || 'Must contain uppercase, lowercase, and number'}
//               InputProps={{
//                 startAdornment: (
//                   <InputAdornment position="start">
//                     <Lock color="action" />
//                   </InputAdornment>
//                 ),
//                 endAdornment: (
//                   <InputAdornment position="end">
//                     <IconButton
//                       onClick={() => setShowPassword(!showPassword)}
//                       edge="end"
//                     >
//                       {showPassword ? <VisibilityOff /> : <Visibility />}
//                     </IconButton>
//                   </InputAdornment>
//                 ),
//               }}
//               sx={{ mb: 3 }}
//               variant="outlined"
//             />

//             {/* Confirm Password */}
//             <TextField
//               fullWidth
//               name="confirmPassword"
//               label="Confirm New Password"
//               type={showConfirmPassword ? 'text' : 'password'}
//               value={formData.confirmPassword}
//               onChange={handleChange}
//               error={!!errors.confirmPassword}
//               helperText={errors.confirmPassword}
//               InputProps={{
//                 startAdornment: (
//                   <InputAdornment position="start">
//                     <Lock color="action" />
//                   </InputAdornment>
//                 ),
//                 endAdornment: (
//                   <InputAdornment position="end">
//                     <IconButton
//                       onClick={() => setShowConfirmPassword(!showConfirmPassword)}
//                       edge="end"
//                     >
//                       {showConfirmPassword ? <VisibilityOff /> : <Visibility />}
//                     </IconButton>
//                   </InputAdornment>
//                 ),
//               }}
//               sx={{ mb: 3 }}
//               variant="outlined"
//             />

//             <Button
//               type="submit"
//               fullWidth
//               variant="contained"
//               size="large"
//               disabled={loading}
//               startIcon={loading ? <CircularProgress size={20} /> : <LockReset />}
//               sx={{
//                 py: 1.5,
//                 borderRadius: 2,
//                 textTransform: 'none',
//                 fontSize: '1.1rem',
//                 fontWeight: 600,
//                 background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
//                 mb: 2
//               }}
//             >
//               {loading ? 'Resetting Password...' : 'Reset Password'}
//             </Button>

//             <Button
//               fullWidth
//               variant="text"
//               onClick={() => setActiveStep(0)}
//               sx={{ textTransform: 'none' }}
//             >
//               Didn't receive the code? Send again
//             </Button>
//           </Box>
//         );
        
//       default:
//         return null;
//     }
//   };

//   return (
//     <Box
//       sx={{
//         minHeight: '100vh',
//         display: 'flex',
//         alignItems: 'center',
//         justifyContent: 'center',
//         background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
//         p: 2
//       }}
//     >
//       <Card
//         elevation={24}
//         sx={{
//           maxWidth: 500,
//           width: '100%',
//           borderRadius: 3,
//           overflow: 'hidden',
//           position: 'relative'
//         }}
//       >
//         <CardContent sx={{ p: 4 }}>
//           {/* Header */}
//           <Box textAlign="center" mb={4}>
//             <Box
//               sx={{
//                 width: 64,
//                 height: 64,
//                 borderRadius: '50%',
//                 background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
//                 display: 'flex',
//                 alignItems: 'center',
//                 justifyContent: 'center',
//                 mx: 'auto',
//                 mb: 2
//               }}
//             >
//               <LockReset sx={{ fontSize: 32, color: 'white' }} />
//             </Box>
//             <Typography variant="h4" fontWeight="bold" gutterBottom>
//               Reset Password
//             </Typography>
//             <Typography variant="body1" color="text.secondary">
//               {activeStep === 0 && 'Forgot your password? No worries!'}
//               {activeStep === 1 && 'Enter the code and create new password'}
//             </Typography>
//           </Box>

//           {/* Progress Stepper */}
//           <Box mb={4}>
//             <Stepper activeStep={activeStep} alternativeLabel>
//               {steps.map((label) => (
//                 <Step key={label}>
//                   <StepLabel>{label}</StepLabel>
//                 </Step>
//               ))}
//             </Stepper>
//           </Box>

//           {/* Success Message */}
//           {successMessage && (
//             <Alert severity="success" sx={{ mb: 3, borderRadius: 2 }}>
//               {successMessage}
//             </Alert>
//           )}

//           {/* Error Alert */}
//           {apiError && (
//             <Alert severity="error" sx={{ mb: 3, borderRadius: 2 }}>
//               {apiError}
//             </Alert>
//           )}

//           {/* Step Content */}
//           <Paper elevation={0} sx={{ p: 3, backgroundColor: 'transparent' }}>
//             {renderStepContent(activeStep)}
//           </Paper>

//           {/* Back to Login */}
//           <Box display="flex" justifyContent="center" mt={4}>
//             <Button
//               variant="text"
//               startIcon={<ArrowBack />}
//               onClick={() => navigate('/login')}
//               sx={{
//                 textTransform: 'none',
//                 color: theme.palette.text.secondary
//               }}
//             >
//               Back to Sign In
//             </Button>
//           </Box>
//         </CardContent>
//       </Card>
//     </Box>
//   );
// };

// export default ForgotPassword;





// src/components/auth/ForgotPassword.jsx
import React, { useState } from 'react';
import {
  Box,
  Container,
  Grid,
  TextField,
  Button,
  Typography,
  Alert,
  InputAdornment,
  CircularProgress,
  useTheme,
  Stepper,
  Step,
  StepLabel,
  Paper,
  IconButton,
  alpha,
  Stack,
  styled,
  StepConnector
} from '@mui/material';
import {
  Email,
  Lock,
  LockReset,
  ArrowBack,
  Send,
  Visibility,
  VisibilityOff,
  Security,
  Check,
  MarkEmailRead,
  VpnKey,
  VideoCall,
  Shield,
  Timer
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../hooks/useAuth';

const steps = ['Enter Email', 'Reset Password'];

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

const ForgotPassword = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const { forgotPassword, resetPassword, loading } = useAuth();
  
  const [activeStep, setActiveStep] = useState(0);
  const [formData, setFormData] = useState({
    email: '',
    otp: '',
    newPassword: '',
    confirmPassword: ''
  });
  
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [errors, setErrors] = useState({});
  const [apiError, setApiError] = useState('');
  const [successMessage, setSuccessMessage] = useState('');

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
    
    if (errors[name]) {
      setErrors(prev => ({ ...prev, [name]: '' }));
    }
  };

  const validateEmail = () => {
    const newErrors = {};
    
    if (!formData.email) {
      newErrors.email = 'Email is required';
    } else if (!/\S+@\S+\.\S+/.test(formData.email)) {
      newErrors.email = 'Please enter a valid email address';
    }
    
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const validateResetForm = () => {
    const newErrors = {};
    
    if (!formData.otp) {
      newErrors.otp = 'OTP is required';
    } else if (formData.otp.length !== 6) {
      newErrors.otp = 'OTP must be 6 digits';
    }
    
    if (!formData.newPassword) {
      newErrors.newPassword = 'New password is required';
    } else if (formData.newPassword.length < 8) {
      newErrors.newPassword = 'Password must be at least 8 characters';
    } else if (!/(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/.test(formData.newPassword)) {
      newErrors.newPassword = 'Password must contain uppercase, lowercase, and number';
    }
    
    if (formData.newPassword !== formData.confirmPassword) {
      newErrors.confirmPassword = 'Passwords do not match';
    }
    
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSendResetEmail = async (e) => {
    e.preventDefault();
    setApiError('');
    
    if (!validateEmail()) return;
    
    try {
      console.log('Sending forgot password request for:', formData.email);
      await forgotPassword(formData.email);
      setSuccessMessage('Reset code sent to your email address');
      setActiveStep(1);
    } catch (error) {
      console.error('Forgot password error:', error);
      setApiError(error.message || 'Failed to send reset email. Please try again.');
    }
  };

  const handleResetPassword = async (e) => {
    e.preventDefault();
    setApiError('');
    
    if (!validateResetForm()) return;
    
    try {
      console.log('Sending reset password request with OTP:', formData.otp);
      await resetPassword({
        otp: formData.otp,
        password: formData.newPassword,
        email: formData.email
      });
      setSuccessMessage('Password reset successfully! You can now sign in with your new password.');
      setTimeout(() => navigate('/login'), 3000);
    } catch (error) {
      console.error('Reset password error:', error);
      setApiError(error.message || 'Failed to reset password. Please try again.');
    }
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box component="form" onSubmit={handleSendResetEmail}>
            <Typography variant="h5" fontWeight={600} color="text.primary" mb={2} textAlign="center">
              Reset Your Password
            </Typography>
            <Typography variant="body1" color="text.secondary" mb={4} textAlign="center">
              Enter your email address and we'll send you a secure 6-digit code to reset your password.
            </Typography>
            
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
              sx={{ mb: 4 }}
              autoFocus
            />

            <Button
              type="submit"
              fullWidth
              variant="contained"
              size="large"
              disabled={loading}
              startIcon={loading ? <CircularProgress size={20} /> : <Send />}
              sx={{
                py: 1.8,
                borderRadius: 2,
                textTransform: 'none',
                fontSize: '1.1rem',
                fontWeight: 600,
                background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                boxShadow: '0 8px 24px rgba(0, 0, 0, 0.15)',
                '&:hover': {
                  background: `linear-gradient(135deg, ${theme.palette.primary.dark}, ${theme.palette.secondary.dark})`,
                  transform: 'translateY(-2px)',
                  boxShadow: '0 12px 32px rgba(0, 0, 0, 0.25)'
                }
              }}
            >
              {loading ? 'Sending Reset Code...' : 'Send Reset Code'}
            </Button>
          </Box>
        );
        
      case 1:
        return (
          <Box component="form" onSubmit={handleResetPassword}>
            <Typography variant="h5" fontWeight={600} color="text.primary" mb={2} textAlign="center">
              Create New Password
            </Typography>
            <Typography variant="body1" color="text.secondary" mb={4} textAlign="center">
              Enter the 6-digit code sent to your email and create your new secure password.
            </Typography>
            
            <Stack spacing={3}>
              {/* OTP Input */}
              <StyledTextField
                fullWidth
                name="otp"
                label="Enter 6-digit Security Code"
                value={formData.otp}
                onChange={handleChange}
                error={!!errors.otp}
                helperText={errors.otp}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Security color="action" />
                    </InputAdornment>
                  ),
                }}
                inputProps={{
                  maxLength: 6,
                  style: { 
                    textAlign: 'center', 
                    fontSize: '1.3rem', 
                    letterSpacing: '0.5rem',
                    fontWeight: 'bold'
                  }
                }}
                autoFocus
              />

              {/* New Password */}
              <StyledTextField
                fullWidth
                name="newPassword"
                label="New Password"
                type={showPassword ? 'text' : 'password'}
                value={formData.newPassword}
                onChange={handleChange}
                error={!!errors.newPassword}
                helperText={errors.newPassword || 'Must contain uppercase, lowercase, and number (min 8 characters)'}
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

              {/* Confirm Password */}
              <StyledTextField
                fullWidth
                name="confirmPassword"
                label="Confirm New Password"
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
            </Stack>

            <Button
              type="submit"
              fullWidth
              variant="contained"
              size="large"
              disabled={loading}
              startIcon={loading ? <CircularProgress size={20} /> : <LockReset />}
              sx={{
                py: 1.8,
                borderRadius: 2,
                textTransform: 'none',
                fontSize: '1.1rem',
                fontWeight: 600,
                background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                boxShadow: '0 8px 24px rgba(0, 0, 0, 0.15)',
                mt: 4,
                mb: 2,
                '&:hover': {
                  background: `linear-gradient(135deg, ${theme.palette.primary.dark}, ${theme.palette.secondary.dark})`,
                  transform: 'translateY(-2px)',
                  boxShadow: '0 12px 32px rgba(0, 0, 0, 0.25)'
                }
              }}
            >
              {loading ? 'Updating Password...' : 'Update Password'}
            </Button>

            <Button
              fullWidth
              variant="text"
              onClick={() => setActiveStep(0)}
              sx={{ 
                textTransform: 'none',
                fontWeight: 500,
                color: theme.palette.text.secondary,
                '&:hover': {
                  backgroundColor: alpha(theme.palette.primary.main, 0.05),
                  color: theme.palette.primary.main
                }
              }}
            >
              Didn't receive the code? Try again
            </Button>
          </Box>
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
      <Container maxWidth="xl" sx={{ height: '100vh', display: 'flex', alignItems: 'center', py: 3 }}>
        <Grid container spacing={0} sx={{ height: '100%', maxHeight: '900px' }}>
          {/* Left Side - Branding */}
          <Grid item xs={12} md={6} lg={7}>
            <Box
              sx={{
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'center',
                px: { xs: 2, md: 6, lg: 8 },
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

              {/* Content */}
              <Typography variant="h2" fontWeight={700} mb={3} sx={{ lineHeight: 1.2 }}>
                Secure Password Recovery
              </Typography>
              
              <Typography variant="h6" sx={{ opacity: 0.9, mb: 4, maxWidth: 600 }}>
                Your account security is our priority. We use advanced encryption 
                and secure verification processes to protect your data.
              </Typography>

              {/* Security Features */}
              <Stack spacing={3} mb={4}>
                {[
                  { icon: <Shield />, title: 'Military-Grade Encryption', desc: 'Your data is protected with AES-256 encryption' },
                  { icon: <Timer />, title: 'Time-Limited Codes', desc: 'Reset codes expire in 15 minutes for security' },
                  { icon: <Security />, title: 'Multi-Factor Verification', desc: 'Additional security layers protect your account' }
                ].map((feature, index) => (
                  <Stack key={index} direction="row" alignItems="flex-start" spacing={2}>
                    <Box
                      sx={{
                        width: 48,
                        height: 48,
                        borderRadius: 2,
                        background: alpha(theme.palette.background.paper, 0.15),
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        flexShrink: 0,
                        mt: 0.5
                      }}
                    >
                      {React.cloneElement(feature.icon, { 
                        sx: { color: 'white', fontSize: 24 } 
                      })}
                    </Box>
                    <Box>
                      <Typography variant="subtitle1" fontWeight={600} sx={{ mb: 0.5 }}>
                        {feature.title}
                      </Typography>
                      <Typography variant="body2" sx={{ opacity: 0.8 }}>
                        {feature.desc}
                      </Typography>
                    </Box>
                  </Stack>
                ))}
              </Stack>

              {/* Help Info */}
              <Paper 
                elevation={0} 
                sx={{ 
                  p: 3, 
                  backgroundColor: alpha(theme.palette.background.paper, 0.1),
                  borderRadius: 3,
                  border: `1px solid ${alpha(theme.palette.common.white, 0.2)}`,
                  backdropFilter: 'blur(10px)'
                }}
              >
                <Typography variant="subtitle2" fontWeight={600} mb={1} color="white">
                  Need Help?
                </Typography>
                <Typography variant="body2" sx={{ opacity: 0.9 }} color="white">
                  Contact our 24/7 support team if you're having trouble accessing your account.
                </Typography>
              </Paper>
            </Box>
          </Grid>

          {/* Right Side - Reset Form */}
          <Grid item xs={12} md={6} lg={5}>
            <Box
              sx={{
                height: '100%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                px: { xs: 2, md: 4 },
                position: 'relative',
                zIndex: 1
              }}
            >
              <Paper
                elevation={0}
                sx={{
                  width: '100%',
                  maxWidth: 520,
                  p: { xs: 3, md: 4 },
                  borderRadius: 3,
                  background: alpha(theme.palette.background.paper, 0.95),
                  backdropFilter: 'blur(20px)',
                  border: `1px solid ${alpha(theme.palette.common.white, 0.2)}`,
                  boxShadow: '0 32px 64px rgba(0, 0, 0, 0.15)',
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
                    Password Recovery
                  </Typography>
                  <Typography variant="body1" color="text.secondary">
                    Secure and easy password reset process
                  </Typography>
                </Box>

                {/* Progress Stepper */}
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
                              fontSize: '1rem',
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

                {/* Success Message */}
                {successMessage && (
                  <Alert 
                    severity="success" 
                    sx={{ 
                      mb: 3, 
                      borderRadius: 2,
                      '& .MuiAlert-message': {
                        fontWeight: 500
                      }
                    }}
                  >
                    {successMessage}
                  </Alert>
                )}

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
                  >
                    {apiError}
                  </Alert>
                )}

                {/* Step Content */}
                <Box sx={{ minHeight: 300, mb: 4 }}>
                  {renderStepContent(activeStep)}
                </Box>

                {/* Back to Login */}
                <Stack direction="row" justifyContent="center">
                  <Button
                    variant="text"
                    startIcon={<ArrowBack />}
                    onClick={() => navigate('/login')}
                    sx={{
                      textTransform: 'none',
                      color: theme.palette.text.secondary,
                      fontWeight: 500,
                      px: 3,
                      py: 1.5,
                      borderRadius: 2,
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.05),
                        color: theme.palette.primary.main
                      }
                    }}
                  >
                    Back to Sign In
                  </Button>
                </Stack>
              </Paper>
            </Box>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
};

export default ForgotPassword;





