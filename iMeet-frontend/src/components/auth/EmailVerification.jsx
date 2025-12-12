// // src/components/auth/EmailVerification.jsx
// import React, { useState, useEffect } from 'react';
// import {
//   Box,
//   Card,
//   CardContent,
//   TextField,
//   Button,
//   Typography,
//   Alert,
//   CircularProgress,
//   useTheme,
//   Chip,
//   Stack
// } from '@mui/material';
// import {
//   Email,
//   CheckCircle,
//   Refresh,
//   ArrowForward,
//   MarkEmailRead
// } from '@mui/icons-material';
// import { useNavigate, useSearchParams } from 'react-router-dom';
// import { useAuth } from '../../hooks/useAuth';

// const EmailVerification = () => {
//   const theme = useTheme();
//   const navigate = useNavigate();
//   const [searchParams] = useSearchParams();
//   const { verifyEmail, resendVerification, loading, user } = useAuth();
  
//   const [verificationCode, setVerificationCode] = useState('');
//   const [errors, setErrors] = useState({});
//   const [apiError, setApiError] = useState('');
//   const [successMessage, setSuccessMessage] = useState('');
//   const [isVerified, setIsVerified] = useState(false);
//   const [countdown, setCountdown] = useState(0);

//   // Get email from URL params or user context
//   const emailFromParams = searchParams.get('email');
//   const userEmail = user?.email || emailFromParams || '';

//   useEffect(() => {
//     // Auto-verify if verification code is in URL
//     const codeFromParams = searchParams.get('code');
//     if (codeFromParams && userEmail) {
//       handleVerification(codeFromParams);
//     }
//   }, [searchParams, userEmail]);

//   useEffect(() => {
//     // Countdown timer for resend button
//     if (countdown > 0) {
//       const timer = setTimeout(() => setCountdown(countdown - 1), 1000);
//       return () => clearTimeout(timer);
//     }
//   }, [countdown]);

//   const handleChange = (e) => {
//     const { value } = e.target;
//     // Only allow numbers and limit to 6 digits
//     const sanitizedValue = value.replace(/\D/g, '').slice(0, 6);
//     setVerificationCode(sanitizedValue);
    
//     // Clear errors when user types
//     if (errors.verificationCode) {
//       setErrors({});
//     }
    
//     // Auto-submit when 6 digits are entered
//     if (sanitizedValue.length === 6) {
//       handleVerification(sanitizedValue);
//     }
//   };

//   const validateCode = (code) => {
//     const newErrors = {};
    
//     if (!code) {
//       newErrors.verificationCode = 'Verification code is required';
//     } else if (code.length !== 6) {
//       newErrors.verificationCode = 'Verification code must be 6 digits';
//     }
    
//     setErrors(newErrors);
//     return Object.keys(newErrors).length === 0;
//   };

//   const handleVerification = async (code = verificationCode) => {
//     setApiError('');
    
//     if (!validateCode(code)) return;
    
//     try {
//       await verifyEmail({
//         email: userEmail,
//         verificationCode: code
//       });
      
//       setIsVerified(true);
//       setSuccessMessage('Email verified successfully! Welcome to our platform.');
      
//       // Redirect to dashboard after 2 seconds
//       setTimeout(() => {
//         navigate('/dashboard');
//       }, 2000);
      
//     } catch (error) {
//       setApiError(error.message || 'Invalid verification code. Please try again.');
//       setVerificationCode('');
//     }
//   };

//   const handleResendCode = async () => {
//     setApiError('');
    
//     try {
//       await resendVerification({ email: userEmail });
//       setSuccessMessage('New verification code sent to your email');
//       setCountdown(60); // 60 second cooldown
//       setVerificationCode('');
//     } catch (error) {
//       setApiError(error.message || 'Failed to resend verification code');
//     }
//   };

//   const handleSubmit = (e) => {
//     e.preventDefault();
//     handleVerification();
//   };

//   if (isVerified) {
//     return (
//       <Box
//         sx={{
//           minHeight: '100vh',
//           display: 'flex',
//           alignItems: 'center',
//           justifyContent: 'center',
//           background: `linear-gradient(135deg, ${theme.palette.success.main} 0%, ${theme.palette.primary.main} 100%)`,
//           p: 2
//         }}
//       >
//         <Card
//           elevation={24}
//           sx={{
//             maxWidth: 500,
//             width: '100%',
//             borderRadius: 3,
//             textAlign: 'center'
//           }}
//         >
//           <CardContent sx={{ p: 4 }}>
//             <Box
//               sx={{
//                 width: 80,
//                 height: 80,
//                 borderRadius: '50%',
//                 background: `linear-gradient(135deg, ${theme.palette.success.main}, ${theme.palette.success.dark})`,
//                 display: 'flex',
//                 alignItems: 'center',
//                 justifyContent: 'center',
//                 mx: 'auto',
//                 mb: 3
//               }}
//             >
//               <CheckCircle sx={{ fontSize: 40, color: 'white' }} />
//             </Box>
            
//             <Typography variant="h4" fontWeight="bold" gutterBottom>
//               Email Verified!
//             </Typography>
            
//             <Typography variant="body1" color="text.secondary" mb={3}>
//               Your email has been successfully verified. You're all set to use our platform.
//             </Typography>
            
//             <Alert severity="success" sx={{ mb: 3, borderRadius: 2 }}>
//               {successMessage}
//             </Alert>
            
//             <Typography variant="body2" color="text.secondary" mb={3}>
//               Redirecting to dashboard in a moment...
//             </Typography>
            
//             <CircularProgress size={24} />
//           </CardContent>
//         </Card>
//       </Box>
//     );
//   }

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
//           overflow: 'hidden'
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
//               <MarkEmailRead sx={{ fontSize: 32, color: 'white' }} />
//             </Box>
            
//             <Typography variant="h4" fontWeight="bold" gutterBottom>
//               Verify Your Email
//             </Typography>
            
//             <Typography variant="body1" color="text.secondary" mb={2}>
//               We've sent a 6-digit verification code to
//             </Typography>
            
//             <Chip
//               icon={<Email />}
//               label={userEmail}
//               color="primary"
//               variant="outlined"
//               sx={{ 
//                 fontSize: '0.9rem',
//                 fontWeight: 500,
//                 px: 1
//               }}
//             />
//           </Box>

//           {/* Success Message */}
//           {successMessage && !isVerified && (
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

//           {/* Verification Form */}
//           <Box component="form" onSubmit={handleSubmit}>
//             <TextField
//               fullWidth
//               label="Enter 6-digit verification code"
//               value={verificationCode}
//               onChange={handleChange}
//               error={!!errors.verificationCode}
//               helperText={errors.verificationCode || 'Code will be verified automatically when complete'}
//               inputProps={{
//                 maxLength: 6,
//                 style: { 
//                   textAlign: 'center', 
//                   fontSize: '1.8rem', 
//                   letterSpacing: '0.5rem',
//                   fontWeight: 'bold'
//                 }
//               }}
//               sx={{ 
//                 mb: 3,
//                 '& .MuiOutlinedInput-root': {
//                   height: '80px'
//                 }
//               }}
//               variant="outlined"
//               autoFocus
//               autoComplete="one-time-code"
//             />

//             <Button
//               type="submit"
//               fullWidth
//               variant="contained"
//               size="large"
//               disabled={loading || verificationCode.length !== 6}
//               startIcon={loading ? <CircularProgress size={20} /> : <CheckCircle />}
//               sx={{
//                 py: 1.5,
//                 borderRadius: 2,
//                 textTransform: 'none',
//                 fontSize: '1.1rem',
//                 fontWeight: 600,
//                 background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
//                 mb: 3,
//                 '&:hover': {
//                   background: `linear-gradient(135deg, ${theme.palette.primary.dark}, ${theme.palette.secondary.dark})`,
//                 }
//               }}
//             >
//               {loading ? 'Verifying...' : 'Verify Email'}
//             </Button>

//             {/* Resend Section */}
//             <Box textAlign="center">
//               <Typography variant="body2" color="text.secondary" mb={2}>
//                 Didn't receive the code?
//               </Typography>
              
//               <Stack direction="row" spacing={2} justifyContent="center">
//                 <Button
//                   variant="outlined"
//                   startIcon={<Refresh />}
//                   onClick={handleResendCode}
//                   disabled={countdown > 0 || loading}
//                   sx={{
//                     textTransform: 'none',
//                     borderRadius: 2
//                   }}
//                 >
//                   {countdown > 0 ? `Resend in ${countdown}s` : 'Resend Code'}
//                 </Button>
                
//                 <Button
//                   variant="text"
//                   endIcon={<ArrowForward />}
//                   onClick={() => navigate('/login')}
//                   sx={{
//                     textTransform: 'none'
//                   }}
//                 >
//                   Back to Login
//                 </Button>
//               </Stack>
//             </Box>
//           </Box>

//           {/* Help Text */}
//           <Box mt={4} p={2} sx={{ backgroundColor: theme.palette.grey[50], borderRadius: 2 }}>
//             <Typography variant="body2" color="text.secondary" textAlign="center">
//               <strong>Having trouble?</strong><br />
//               Check your spam folder or contact support if you don't receive the code within 5 minutes.
//             </Typography>
//           </Box>
//         </CardContent>
//       </Card>
//     </Box>
//   );
// };

// export default EmailVerification;




// src/components/auth/EmailVerification.jsx
import React, { useState, useEffect } from 'react';
import {
  Card,
  Box,
  Container,
  Grid,
  TextField,
  Button,
  Typography,
  Alert,
  CircularProgress,
  useTheme,
  Chip,
  Stack,
  Paper,
  alpha,
  styled,
  Fade,
  Zoom,
  LinearProgress,
  Divider
} from '@mui/material';
import {
  Email,
  CheckCircle,
  Refresh,
  ArrowForward,
  MarkEmailRead,
  Verified,
  Timer,
  Security,
  Support,
  Dashboard,
  VideoCall,
  Shield,
  Speed
} from '@mui/icons-material';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../../hooks/useAuth';

const StyledTextField = styled(TextField)(({ theme }) => ({
  '& .MuiOutlinedInput-root': {
    borderRadius: 16,
    backgroundColor: alpha(theme.palette.background.paper, 0.9),
    transition: 'all 0.3s ease',
    height: 80,
    fontSize: '2rem',
    fontWeight: 'bold',
    '&:hover': {
      backgroundColor: theme.palette.background.paper,
      '& .MuiOutlinedInput-notchedOutline': {
        borderColor: alpha(theme.palette.primary.main, 0.5),
        borderWidth: 2,
      },
    },
    '&.Mui-focused': {
      backgroundColor: theme.palette.background.paper,
      boxShadow: `0 0 0 4px ${alpha(theme.palette.primary.main, 0.15)}`,
      '& .MuiOutlinedInput-notchedOutline': {
        borderColor: theme.palette.primary.main,
        borderWidth: 3,
      },
    },
  },
  '& .MuiInputLabel-root': {
    fontWeight: 600,
    fontSize: '1.1rem',
  },
}));

const SuccessCard = styled(Card)(({ theme }) => ({
  background: `linear-gradient(135deg, ${theme.palette.success.main}, ${theme.palette.success.dark})`,
  color: 'white',
  borderRadius: 24,
  overflow: 'hidden',
  position: 'relative',
  '&::before': {
    content: '""',
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.1"%3E%3Ccircle cx="30" cy="30" r="2"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
  }
}));

const EmailVerification = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { verifyEmail, resendVerification, loading, user } = useAuth();
  
  const [verificationCode, setVerificationCode] = useState('');
  const [errors, setErrors] = useState({});
  const [apiError, setApiError] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [isVerified, setIsVerified] = useState(false);
  const [countdown, setCountdown] = useState(0);
  const [timeLeft, setTimeLeft] = useState(15 * 60); // 15 minutes

  // Get email from URL params or user context
  const emailFromParams = searchParams.get('email');
  const userEmail = user?.email || emailFromParams || '';

  useEffect(() => {
    // Auto-verify if verification code is in URL
    const codeFromParams = searchParams.get('code');
    if (codeFromParams && userEmail) {
      handleVerification(codeFromParams);
    }
  }, [searchParams, userEmail]);

  useEffect(() => {
    // Countdown timer for resend button
    if (countdown > 0) {
      const timer = setTimeout(() => setCountdown(countdown - 1), 1000);
      return () => clearTimeout(timer);
    }
  }, [countdown]);

  useEffect(() => {
    // Timer for code expiration
    if (timeLeft > 0 && !isVerified) {
      const timer = setTimeout(() => setTimeLeft(timeLeft - 1), 1000);
      return () => clearTimeout(timer);
    }
  }, [timeLeft, isVerified]);

  const formatTime = (seconds) => {
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`;
  };

  const handleChange = (e) => {
    const { value } = e.target;
    // Only allow numbers and limit to 6 digits
    const sanitizedValue = value.replace(/\D/g, '').slice(0, 6);
    setVerificationCode(sanitizedValue);
    
    // Clear errors when user types
    if (errors.verificationCode) {
      setErrors({});
    }
    
    // Auto-submit when 6 digits are entered
    if (sanitizedValue.length === 6) {
      handleVerification(sanitizedValue);
    }
  };

  const validateCode = (code) => {
    const newErrors = {};
    
    if (!code) {
      newErrors.verificationCode = 'Verification code is required';
    } else if (code.length !== 6) {
      newErrors.verificationCode = 'Verification code must be 6 digits';
    }
    
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleVerification = async (code = verificationCode) => {
    setApiError('');
    
    if (!validateCode(code)) return;
    
    try {
      await verifyEmail({
        email: userEmail,
        verificationCode: code
      });
      
      setIsVerified(true);
      setSuccessMessage('Email verified successfully! Welcome to our platform.');
      
      // Redirect to dashboard after 3 seconds
      setTimeout(() => {
        navigate('/dashboard');
      }, 3000);
      
    } catch (error) {
      setApiError(error.message || 'Invalid verification code. Please try again.');
      setVerificationCode('');
    }
  };

  const handleResendCode = async () => {
    setApiError('');
    
    try {
      await resendVerification({ email: userEmail });
      setSuccessMessage('New verification code sent to your email');
      setCountdown(60); // 60 second cooldown
      setTimeLeft(15 * 60); // Reset timer
      setVerificationCode('');
    } catch (error) {
      setApiError(error.message || 'Failed to resend verification code');
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    handleVerification();
  };

  if (isVerified) {
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
          background: `linear-gradient(135deg, ${theme.palette.success.main} 0%, ${theme.palette.primary.main} 100%)`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          '&::before': {
            content: '""',
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.05"%3E%3Ccircle cx="30" cy="30" r="2"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
          },
          overflow: 'auto'
        }}
      >
        <Container maxWidth="md">
          <Zoom in timeout={1000}>
            <Paper
              elevation={0}
              sx={{
                p: 6,
                borderRadius: 4,
                textAlign: 'center',
                background: alpha(theme.palette.background.paper, 0.95),
                backdropFilter: 'blur(20px)',
                border: `1px solid ${alpha(theme.palette.common.white, 0.2)}`,
                boxShadow: '0 32px 64px rgba(0, 0, 0, 0.15)',
              }}
            >
              <Zoom in timeout={1500}>
                <Box
                  sx={{
                    width: 100,
                    height: 100,
                    borderRadius: '50%',
                    background: `linear-gradient(135deg, ${theme.palette.success.main}, ${theme.palette.success.dark})`,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    mx: 'auto',
                    mb: 3,
                    boxShadow: '0 16px 32px rgba(0, 0, 0, 0.2)',
                  }}
                >
                  <CheckCircle sx={{ fontSize: 60, color: 'white' }} />
                </Box>
              </Zoom>
              
              <Typography variant="h3" fontWeight={700} gutterBottom color="text.primary">
                Email Verified Successfully!
              </Typography>
              
              <Typography variant="h6" color="text.secondary" mb={3}>
                Your email has been verified and your account is now active
              </Typography>
              
              <Alert 
                severity="success" 
                sx={{ 
                  mb: 4, 
                  borderRadius: 2,
                  textAlign: 'left',
                  '& .MuiAlert-message': {
                    fontWeight: 500,
                    fontSize: '1rem'
                  }
                }}
              >
                {successMessage}
              </Alert>
              
              <Stack direction="row" alignItems="center" justifyContent="center" spacing={1} mb={3}>
                <Dashboard sx={{ color: theme.palette.primary.main }} />
                <Typography variant="body1" color="text.secondary" fontWeight={500}>
                  Redirecting to dashboard in a moment...
                </Typography>
              </Stack>
              
              <CircularProgress 
                size={40} 
                sx={{ 
                  color: theme.palette.primary.main,
                  '& .MuiCircularProgress-circle': {
                    strokeLinecap: 'round',
                  }
                }} 
              />
            </Paper>
          </Zoom>
        </Container>
      </Box>
    );
  }

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

            {/* Content */}
            <Typography variant="h2" fontWeight={700} mb={3} sx={{ lineHeight: 1.2 }}>
              Almost There! 
              <br />Verify Your Email
            </Typography>
            
            <Typography variant="h6" sx={{ opacity: 0.9, mb: 4, maxWidth: 600 }}>
              We've sent a secure verification code to your email. This extra step 
              ensures your account remains safe and accessible only to you.
            </Typography>

            {/* Security Features */}
            <Stack spacing={3} mb={4}>
              {[
                { icon: <Security />, title: 'Secure Verification', desc: 'Military-grade encryption protects your data' },
                { icon: <Speed />, title: 'Quick Process', desc: 'Verification takes less than 2 minutes' },
                { icon: <Shield />, title: 'Account Protection', desc: 'Prevents unauthorized access to your account' }
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

            {/* Stats */}
            <Grid container spacing={4}>
              {[
                { value: '10M+', label: 'Verified Users' },
                { value: '<30s', label: 'Average Time' },
                { value: '99.9%', label: 'Success Rate' }
              ].map((stat, index) => (
                <Grid item xs={4} key={index}>
                  <Typography variant="h4" fontWeight={700}>
                    {stat.value}
                  </Typography>
                  <Typography variant="body2" sx={{ opacity: 0.8 }}>
                    {stat.label}
                  </Typography>
                </Grid>
              ))}
            </Grid>
          </Box>
        </Grid>

        {/* Right Side - Verification Form */}
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
                Verify Your Email
              </Typography>
              
              <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
                We've sent a 6-digit verification code to
              </Typography>
              
              <Chip
                icon={<Email />}
                label={userEmail}
                color="primary"
                variant="outlined"
                sx={{ 
                  fontSize: '1rem',
                  fontWeight: 600,
                  px: 2,
                  py: 1,
                  height: 'auto'
                }}
              />
            </Box>

            {/* Timer and Progress */}
            <Paper 
              elevation={0} 
              sx={{ 
                p: 3, 
                backgroundColor: alpha(theme.palette.warning.main, 0.05),
                borderRadius: 2,
                border: `1px solid ${alpha(theme.palette.warning.main, 0.2)}`,
                mb: 4
              }}
            >
              <Stack direction="row" alignItems="center" justifyContent="space-between" mb={2}>
                <Stack direction="row" alignItems="center" spacing={1}>
                  <Timer sx={{ color: theme.palette.warning.main }} />
                  <Typography variant="subtitle2" fontWeight={600} color="warning.main">
                    Code expires in {formatTime(timeLeft)}
                  </Typography>
                </Stack>
                <Chip 
                  icon={<Verified />}
                  label="Secure" 
                  size="small" 
                  color="primary" 
                  variant="outlined"
                />
              </Stack>
              <LinearProgress 
                variant="determinate" 
                value={(timeLeft / (15 * 60)) * 100}
                sx={{ 
                  height: 6, 
                  borderRadius: 3,
                  backgroundColor: alpha(theme.palette.warning.main, 0.2),
                  '& .MuiLinearProgress-bar': {
                    backgroundColor: theme.palette.warning.main,
                    borderRadius: 3,
                  }
                }}
              />
            </Paper>

            {/* Success Message */}
            {successMessage && !isVerified && (
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

            {/* Verification Form */}
            <Box component="form" onSubmit={handleSubmit} sx={{ mb: 4 }}>
              <Typography variant="h6" fontWeight={600} textAlign="center" mb={3}>
                Enter Verification Code
              </Typography>
              
              <StyledTextField
                fullWidth
                label="6-digit verification code"
                value={verificationCode}
                onChange={handleChange}
                error={!!errors.verificationCode}
                helperText={errors.verificationCode || 'Code will be verified automatically when complete'}
                inputProps={{
                  maxLength: 6,
                  style: { 
                    textAlign: 'center', 
                    fontSize: '2rem', 
                    letterSpacing: '0.8rem',
                    fontWeight: 'bold'
                  }
                }}
                sx={{ mb: 4 }}
                autoFocus
                autoComplete="one-time-code"
              />

              <Button
                type="submit"
                fullWidth
                variant="contained"
                size="large"
                disabled={loading || verificationCode.length !== 6}
                startIcon={loading ? <CircularProgress size={20} /> : <CheckCircle />}
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
                {loading ? 'Verifying...' : 'Verify Email Address'}
              </Button>
            </Box>

            {/* Resend Section */}
            <Paper 
              elevation={0} 
              sx={{ 
                p: 3, 
                backgroundColor: alpha(theme.palette.background.default, 0.5),
                borderRadius: 2,
                textAlign: 'center',
                mb: 4
              }}
            >
              <Typography variant="body1" color="text.secondary" mb={2} fontWeight={500}>
                Didn't receive the verification code?
              </Typography>
              
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} justifyContent="center">
                <Button
                  variant="outlined"
                  startIcon={<Refresh />}
                  onClick={handleResendCode}
                  disabled={countdown > 0 || loading}
                  sx={{
                    textTransform: 'none',
                    borderRadius: 2,
                    px: 3,
                    py: 1.5,
                    fontWeight: 600,
                    borderColor: alpha(theme.palette.primary.main, 0.3),
                    '&:hover': {
                      borderColor: theme.palette.primary.main,
                      backgroundColor: alpha(theme.palette.primary.main, 0.05)
                    }
                  }}
                >
                  {countdown > 0 ? `Resend in ${countdown}s` : 'Resend Code'}
                </Button>
                
                <Button
                  variant="text"
                  endIcon={<ArrowForward />}
                  onClick={() => navigate('/login')}
                  sx={{
                    textTransform: 'none',
                    px: 3,
                    py: 1.5,
                    fontWeight: 600,
                    color: theme.palette.text.secondary,
                    '&:hover': {
                      backgroundColor: alpha(theme.palette.primary.main, 0.05),
                      color: theme.palette.primary.main
                    }
                  }}
                >
                  Back to Login
                </Button>
              </Stack>
            </Paper>

            {/* Help Section */}
            <Paper 
              elevation={0} 
              sx={{ 
                p: 3, 
                backgroundColor: alpha(theme.palette.info.main, 0.05),
                borderRadius: 2,
                border: `1px solid ${alpha(theme.palette.info.main, 0.1)}`
              }}
            >
              <Stack direction="row" alignItems="center" justifyContent="center" spacing={1} mb={2}>
                <Support sx={{ color: theme.palette.info.main }} />
                <Typography variant="subtitle1" fontWeight={600} color="info.main">
                  Need Help?
                </Typography>
              </Stack>
              
              <Typography variant="body2" color="text.secondary" textAlign="center" lineHeight={1.6}>
                Check your spam/junk folder or contact our support team if you don't receive the code within 5 minutes.
              </Typography>
            </Paper>
          </Box>
        </Grid>
      </Grid>
    </Box>
  );
};

export default EmailVerification;