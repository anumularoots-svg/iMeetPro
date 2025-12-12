// // src/components/auth/AuthWrapper.jsx
// import React from 'react';
// import {
//   Box,
//   Container,
//   Typography,
//   Card,
//   CardContent,
//   useTheme,
//   useMediaQuery,
//   Fade,
//   Slide
// } from '@mui/material';
// import {
//   VideoCall,
//   Security,
//   CloudSync,
//   Speed,
//   Group,
//   Star
// } from '@mui/icons-material';

// const features = [
//   {
//     icon: <VideoCall />,
//     title: 'HD Video Calls',
//     description: 'Crystal clear video quality with smart bandwidth optimization'
//   },
//   {
//     icon: <Security />,
//     title: 'End-to-End Security',
//     description: 'Your meetings are protected with enterprise-grade encryption'
//   },
//   {
//     icon: <CloudSync />,
//     title: 'Cloud Integration',
//     description: 'Seamlessly sync with Google Calendar, Outlook, and more'
//   },
//   {
//     icon: <Speed />,
//     title: 'Lightning Fast',
//     description: 'Join meetings instantly with our optimized infrastructure'
//   },
//   {
//     icon: <Group />,
//     title: 'Team Collaboration',
//     description: 'Advanced features for productive team meetings'
//   },
//   {
//     icon: <Star />,
//     title: 'AI-Powered',
//     description: 'Smart attendance tracking and engagement analytics'
//   }
// ];

// const testimonials = [
//   {
//     name: 'Sarah Johnson',
//     role: 'Product Manager',
//     company: 'TechCorp',
//     text: 'The best video meeting platform we\'ve used. Clean interface and reliable performance.',
//     rating: 5
//   },
//   {
//     name: 'Michael Chen',
//     role: 'Remote Team Lead',
//     company: 'InnovateLab',
//     text: 'AI attendance tracking has revolutionized how we manage remote meetings.',
//     rating: 5
//   },
//   {
//     name: 'Emily Rodriguez',
//     role: 'HR Director',
//     company: 'Global Solutions',
//     text: 'Seamless calendar integration saves us hours of scheduling coordination.',
//     rating: 5
//   }
// ];

// const AuthWrapper = ({ children, showFeatures = true }) => {
//   const theme = useTheme();
//   const isMobile = useMediaQuery(theme.breakpoints.down('lg'));
//   const isTablet = useMediaQuery(theme.breakpoints.down('md'));

//   if (isMobile) {
//     // Mobile layout - single column
//     return (
//       <Box
//         sx={{
//           minHeight: '100vh',
//           background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
//           display: 'flex',
//           flexDirection: 'column'
//         }}
//       >
//         {/* Mobile Header */}
//         <Box sx={{ p: 2, textAlign: 'center' }}>
//           <Typography
//             variant="h4"
//             fontWeight="bold"
//             color="white"
//             gutterBottom
//           >
//             MeetingPro
//           </Typography>
//           <Typography variant="body2" color="white" opacity={0.9}>
//             Professional Video Meetings Made Simple
//           </Typography>
//         </Box>

//         {/* Main Content */}
//         <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', p: 2 }}>
//           {children}
//         </Box>
//       </Box>
//     );
//   }

//   return (
//     <Box
//       sx={{
//         minHeight: '100vh',
//         display: 'flex',
//         background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`
//       }}
//     >
//       {/* Left Side - Features & Branding */}
//       {showFeatures && (
//         <Box
//           sx={{
//             flex: isTablet ? 0.4 : 0.6,
//             display: 'flex',
//             flexDirection: 'column',
//             justifyContent: 'center',
//             p: 4,
//             color: 'white',
//             position: 'relative',
//             overflow: 'hidden'
//           }}
//         >
//           {/* Background Pattern */}
//           <Box
//             sx={{
//               position: 'absolute',
//               top: 0,
//               left: 0,
//               right: 0,
//               bottom: 0,
//               background: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.05"%3E%3Ccircle cx="30" cy="30" r="2"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
//               opacity: 0.3
//             }}
//           />

//           <Container maxWidth="sm" sx={{ position: 'relative', zIndex: 1 }}>
//             {/* Logo & Brand */}
//             <Fade in timeout={1000}>
//               <Box mb={6}>
//                 <Box
//                   sx={{
//                     width: 80,
//                     height: 80,
//                     borderRadius: '20px',
//                     background: 'rgba(255, 255, 255, 0.15)',
//                     backdropFilter: 'blur(10px)',
//                     display: 'flex',
//                     alignItems: 'center',
//                     justifyContent: 'center',
//                     mb: 3,
//                     border: '1px solid rgba(255, 255, 255, 0.2)'
//                   }}
//                 >
//                   <VideoCall sx={{ fontSize: 40, color: 'white' }} />
//                 </Box>
                
//                 <Typography variant="h3" fontWeight="bold" gutterBottom>
//                   MeetingPro
//                 </Typography>
                
//                 <Typography variant="h6" opacity={0.9} mb={2}>
//                   The Future of Video Meetings
//                 </Typography>
                
//                 <Typography variant="body1" opacity={0.8} lineHeight={1.6}>
//                   Connect, collaborate, and create with our next-generation video meeting platform. 
//                   Built for teams that demand reliability, security, and seamless user experience.
//                 </Typography>
//               </Box>
//             </Fade>

//             {/* Features Grid */}
//             <Box>
//               {features.slice(0, isTablet ? 3 : 6).map((feature, index) => (
//                 <Slide
//                   key={feature.title}
//                   direction="right"
//                   in
//                   timeout={1000 + (index * 200)}
//                 >
//                   <Box
//                     sx={{
//                       display: 'flex',
//                       alignItems: 'center',
//                       mb: 3,
//                       p: 2,
//                       borderRadius: 2,
//                       background: 'rgba(255, 255, 255, 0.1)',
//                       backdropFilter: 'blur(10px)',
//                       border: '1px solid rgba(255, 255, 255, 0.1)',
//                       transition: 'all 0.3s ease',
//                       '&:hover': {
//                         background: 'rgba(255, 255, 255, 0.15)',
//                         transform: 'translateX(10px)'
//                       }
//                     }}
//                   >
//                     <Box
//                       sx={{
//                         width: 48,
//                         height: 48,
//                         borderRadius: '12px',
//                         background: 'rgba(255, 255, 255, 0.2)',
//                         display: 'flex',
//                         alignItems: 'center',
//                         justifyContent: 'center',
//                         mr: 2,
//                         flexShrink: 0
//                       }}
//                     >
//                       {React.cloneElement(feature.icon, { sx: { color: 'white' } })}
//                     </Box>
                    
//                     <Box>
//                       <Typography variant="subtitle1" fontWeight="600" gutterBottom>
//                         {feature.title}
//                       </Typography>
//                       <Typography variant="body2" opacity={0.8} fontSize="0.85rem">
//                         {feature.description}
//                       </Typography>
//                     </Box>
//                   </Box>
//                 </Slide>
//               ))}
//             </Box>

//             {/* Stats */}
//             <Fade in timeout={2000}>
//               <Box
//                 sx={{
//                   display: 'flex',
//                   justifyContent: 'space-around',
//                   mt: 4,
//                   p: 3,
//                   borderRadius: 3,
//                   background: 'rgba(255, 255, 255, 0.1)',
//                   backdropFilter: 'blur(10px)',
//                   border: '1px solid rgba(255, 255, 255, 0.1)'
//                 }}
//               >
//                 <Box textAlign="center">
//                   <Typography variant="h4" fontWeight="bold">
//                     50K+
//                   </Typography>
//                   <Typography variant="body2" opacity={0.8}>
//                     Active Users
//                   </Typography>
//                 </Box>
//                 <Box textAlign="center">
//                   <Typography variant="h4" fontWeight="bold">
//                     99.9%
//                   </Typography>
//                   <Typography variant="body2" opacity={0.8}>
//                     Uptime
//                   </Typography>
//                 </Box>
//                 <Box textAlign="center">
//                   <Typography variant="h4" fontWeight="bold">
//                     24/7
//                   </Typography>
//                   <Typography variant="body2" opacity={0.8}>
//                     Support
//                   </Typography>
//                 </Box>
//               </Box>
//             </Fade>
//           </Container>
//         </Box>
//       )}

//       {/* Right Side - Auth Forms */}
//       <Box
//         sx={{
//           flex: showFeatures ? (isTablet ? 0.6 : 0.4) : 1,
//           display: 'flex',
//           alignItems: 'center',
//           justifyContent: 'center',
//           p: 3,
//           position: 'relative'
//         }}
//       >
//         {/* Background Blur Effect */}
//         <Box
//           sx={{
//             position: 'absolute',
//             top: 0,
//             left: 0,
//             right: 0,
//             bottom: 0,
//             background: 'rgba(255, 255, 255, 0.05)',
//             backdropFilter: 'blur(20px)'
//           }}
//         />
        
//         {/* Content Container */}
//         <Box
//           sx={{
//             position: 'relative',
//             zIndex: 1,
//             width: '100%',
//             maxWidth: '500px'
//           }}
//         >
//           <Fade in timeout={1500}>
//             <div>
//               {children}
//             </div>
//           </Fade>
//         </Box>
//       </Box>
//     </Box>
//   );
// };

// // Enhanced version with testimonials
// export const AuthWrapperWithTestimonials = ({ children }) => {
//   const theme = useTheme();
//   const isMobile = useMediaQuery(theme.breakpoints.down('md'));

//   if (isMobile) {
//     return <AuthWrapper>{children}</AuthWrapper>;
//   }

//   return (
//     <Box
//       sx={{
//         minHeight: '100vh',
//         display: 'flex',
//         background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`
//       }}
//     >
//       {/* Left Side - Enhanced with Testimonials */}
//       <Box
//         sx={{
//           flex: 0.6,
//           display: 'flex',
//           flexDirection: 'column',
//           justifyContent: 'space-between',
//           p: 4,
//           color: 'white',
//           position: 'relative',
//           overflow: 'hidden'
//         }}
//       >
//         {/* Brand Section */}
//         <Box>
//           <Fade in timeout={1000}>
//             <Box mb={4}>
//               <Box
//                 sx={{
//                   width: 80,
//                   height: 80,
//                   borderRadius: '20px',
//                   background: 'rgba(255, 255, 255, 0.15)',
//                   backdropFilter: 'blur(10px)',
//                   display: 'flex',
//                   alignItems: 'center',
//                   justifyContent: 'center',
//                   mb: 3
//                 }}
//               >
//                 <VideoCall sx={{ fontSize: 40 }} />
//               </Box>
              
//               <Typography variant="h3" fontWeight="bold" gutterBottom>
//                 MeetingPro
//               </Typography>
              
//               <Typography variant="h6" opacity={0.9}>
//                 Trusted by 50,000+ professionals worldwide
//               </Typography>
//             </Box>
//           </Fade>
//         </Box>

//         {/* Testimonials Carousel */}
//         <Box flex={1} display="flex" flexDirection="column" justifyContent="center">
//           {testimonials.map((testimonial, index) => (
//             <Slide key={index} direction="up" in timeout={1500 + (index * 300)}>
//               <Card
//                 sx={{
//                   mb: 3,
//                   background: 'rgba(255, 255, 255, 0.1)',
//                   backdropFilter: 'blur(10px)',
//                   border: '1px solid rgba(255, 255, 255, 0.2)',
//                   color: 'white'
//                 }}
//               >
//                 <CardContent>
//                   <Box display="flex" mb={2}>
//                     {[...Array(testimonial.rating)].map((_, i) => (
//                       <Star key={i} sx={{ color: '#FFD700', fontSize: 20 }} />
//                     ))}
//                   </Box>
                  
//                   <Typography variant="body1" mb={2} fontStyle="italic">
//                     "{testimonial.text}"
//                   </Typography>
                  
//                   <Typography variant="subtitle2" fontWeight="bold">
//                     {testimonial.name}
//                   </Typography>
//                   <Typography variant="body2" opacity={0.8}>
//                     {testimonial.role} • {testimonial.company}
//                   </Typography>
//                 </CardContent>
//               </Card>
//             </Slide>
//           ))}
//         </Box>
//       </Box>

//       {/* Right Side - Auth Forms */}
//       <Box
//         sx={{
//           flex: 0.4,
//           display: 'flex',
//           alignItems: 'center',
//           justifyContent: 'center',
//           p: 3,
//           position: 'relative'
//         }}
//       >
//         <Box
//           sx={{
//             position: 'absolute',
//             top: 0,
//             left: 0,
//             right: 0,
//             bottom: 0,
//             background: 'rgba(255, 255, 255, 0.05)',
//             backdropFilter: 'blur(20px)'
//           }}
//         />
        
//         <Box
//           sx={{
//             position: 'relative',
//             zIndex: 1,
//             width: '100%',
//             maxWidth: '450px'
//           }}
//         >
//           <Fade in timeout={1500}>
//             <div>{children}</div>
//           </Fade>
//         </Box>
//       </Box>
//     </Box>
//   );
// };

// export default AuthWrapper;




// src/components/auth/AuthWrapper.jsx
import React from 'react';
import {
  Box,
  Container,
  Typography,
  useTheme,
  useMediaQuery,
  Stack,
  Paper,
  alpha,
  styled,
  keyframes
} from '@mui/material';
import {
  VideoCall,
  Security,
  Speed,
  Shield,
  Verified,
  Star
} from '@mui/icons-material';

// Animated background particles
const float = keyframes`
  0%, 100% { transform: translateY(0px) rotate(0deg); }
  50% { transform: translateY(-20px) rotate(180deg); }
`;

const FloatingElement = styled(Box)(({ theme }) => ({
  position: 'absolute',
  borderRadius: '50%',
  background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.1)}, ${alpha(theme.palette.secondary.main, 0.1)})`,
  animation: `${float} 6s ease-in-out infinite`,
  backdropFilter: 'blur(10px)',
}));

const AuthWrapper = ({ children, showFeatures = true }) => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));

  // For mobile and when features are disabled, just return the children with simple background
  if (isMobile || !showFeatures) {
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
            background: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.05"%3E%3Ccircle cx="30" cy="30" r="1.5"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
            },
            overflow: 'auto'
        }}
      >
        <Container maxWidth="sm" sx={{ position: 'relative', zIndex: 1 }}>
          {children}
        </Container>
      </Box>
    );
  }

  // Desktop layout with features
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
        display: 'flex',
        background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
        overflow: 'hidden'
      }}
    >
      {/* Animated Background Elements */}
      <FloatingElement 
        sx={{ 
          width: 100, 
          height: 100, 
          top: '10%', 
          left: '5%',
          animationDelay: '0s'
        }} 
      />
      <FloatingElement 
        sx={{ 
          width: 150, 
          height: 150, 
          top: '60%', 
          left: '10%',
          animationDelay: '2s'
        }} 
      />
      <FloatingElement 
        sx={{ 
          width: 80, 
          height: 80, 
          top: '20%', 
          right: '15%',
          animationDelay: '4s'
        }} 
      />

      {/* Left Side - Simple Branding */}
      <Box
        sx={{
          flex: 0.4,
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          p: 6,
          color: 'white',
          position: 'relative',
          zIndex: 1
        }}
      >
        <Container maxWidth="sm">
          {/* Logo & Brand */}
          <Stack direction="row" alignItems="center" spacing={2} mb={4}>
            <Box
              sx={{
                width: 70,
                height: 70,
                borderRadius: 3,
                background: alpha(theme.palette.background.paper, 0.15),
                backdropFilter: 'blur(20px)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                border: `2px solid ${alpha(theme.palette.common.white, 0.2)}`,
                boxShadow: '0 16px 32px rgba(0, 0, 0, 0.2)'
              }}
            >
              <VideoCall sx={{ fontSize: 36, color: 'white' }} />
            </Box>
            
            <Box>
              <Typography variant="h2" fontWeight={700} sx={{ lineHeight: 1 }}>
                iMeetPro
              </Typography>
              <Stack direction="row" spacing={1} mt={1}>
                <Star sx={{ color: '#FFD700', fontSize: 20 }} />
                <Star sx={{ color: '#FFD700', fontSize: 20 }} />
                <Star sx={{ color: '#FFD700', fontSize: 20 }} />
                <Star sx={{ color: '#FFD700', fontSize: 20 }} />
                <Star sx={{ color: '#FFD700', fontSize: 20 }} />
              </Stack>
            </Box>
          </Stack>
          
          <Typography variant="h4" sx={{ opacity: 0.95, fontWeight: 600, mb: 3 }}>
            The Professional Choice for Video Meetings
          </Typography>
          
          <Typography variant="body1" sx={{ opacity: 0.8, lineHeight: 1.7, fontSize: '1.1rem', mb: 4 }}>
            Join millions of professionals who trust MeetingPro for secure, 
            high-quality video conferencing and collaboration.
          </Typography>

          {/* Key Features */}
          <Stack spacing={2} mb={4}>
            {[
              { icon: <Security />, text: 'Bank-grade security & encryption' },
              { icon: <Speed />, text: 'Lightning-fast global performance' },
              { icon: <Shield />, text: 'SOC 2 Type II certified' },
              { icon: <Verified />, text: 'Trusted by Fortune 500 companies' }
            ].map((feature, index) => (
              <Stack key={index} direction="row" alignItems="center" spacing={2}>
                {React.cloneElement(feature.icon, { 
                  sx: { color: alpha(theme.palette.common.white, 0.9), fontSize: 24 } 
                })}
                <Typography variant="body1" sx={{ opacity: 0.9, fontWeight: 500 }}>
                  {feature.text}
                </Typography>
              </Stack>
            ))}
          </Stack>

          {/* Trust Indicators */}
          <Paper
            elevation={0}
            sx={{
              p: 3,
              borderRadius: 3,
              background: alpha(theme.palette.background.paper, 0.1),
              backdropFilter: 'blur(20px)',
              border: `1px solid ${alpha(theme.palette.common.white, 0.2)}`
            }}
          >
            <Typography variant="h6" fontWeight={600} mb={2} color="white" textAlign="center">
              Trusted Worldwide
            </Typography>
            
            <Stack direction="row" justifyContent="space-around" textAlign="center">
              <Box>
                <Typography variant="h4" fontWeight={700} color="white">
                  10M+
                </Typography>
                <Typography variant="body2" sx={{ opacity: 0.8 }} color="white">
                  Users
                </Typography>
              </Box>
              <Box>
                <Typography variant="h4" fontWeight={700} color="white">
                  99.9%
                </Typography>
                <Typography variant="body2" sx={{ opacity: 0.8 }} color="white">
                  Uptime
                </Typography>
              </Box>
              <Box>
                <Typography variant="h4" fontWeight={700} color="white">
                  150+
                </Typography>
                <Typography variant="body2" sx={{ opacity: 0.8 }} color="white">
                  Countries
                </Typography>
              </Box>
            </Stack>
          </Paper>
        </Container>
      </Box>

      {/* Right Side - Auth Forms */}
      <Box
        sx={{
          flex: 0.6,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          position: 'relative',
          zIndex: 1,
          '&::before': {
            content: '""',
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: alpha(theme.palette.background.paper, 0.05),
            backdropFilter: 'blur(20px)'
          }
        }}
      >
        <Box
          sx={{
            position: 'relative',
            zIndex: 1,
            width: '100%',
            height: '100%',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center'
          }}
        >
          {children}
        </Box>
      </Box>
    </Box>
  );
};

// Enhanced version with testimonials - simplified for better performance
export const AuthWrapperWithTestimonials = ({ children }) => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));

  if (isMobile) {
    return <AuthWrapper showFeatures={false}>{children}</AuthWrapper>;
  }

  return <AuthWrapper showFeatures={true}>{children}</AuthWrapper>;
};

export default AuthWrapper;