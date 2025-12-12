import React from 'react';
import { 
  Box, 
  Typography, 
  Link, 
  Divider, 
  Container,
  Grid,
  IconButton,
  Tooltip
} from '@mui/material';
import { 
  VideoCall,
  Email,
  Phone,
  LocationOn,
  Facebook,
  Twitter,
  LinkedIn,
  Instagram,
  GitHub
} from '@mui/icons-material';

const Footer = () => {
  const currentYear = new Date().getFullYear();

  const footerSections = [
    {
      title: 'Product',
      links: [
        { text: 'Features', href: '#features' },
        { text: 'Pricing', href: '#pricing' },
        { text: 'Security', href: '#security' },
        { text: 'Updates', href: '#updates' }
      ]
    },
    {
      title: 'Support',
      links: [
        { text: 'Help Center', href: '#help' },
        { text: 'Contact Us', href: '#contact' },
        { text: 'API Docs', href: '#api' },
        { text: 'Status', href: '#status' }
      ]
    },
    {
      title: 'Company',
      links: [
        { text: 'About Us', href: '#about' },
        { text: 'Careers', href: '#careers' },
        { text: 'Blog', href: '#blog' },
        { text: 'Press', href: '#press' }
      ]
    },
    {
      title: 'Legal',
      links: [
        { text: 'Privacy Policy', href: '#privacy' },
        { text: 'Terms of Service', href: '#terms' },
        { text: 'Cookie Policy', href: '#cookies' },
        { text: 'GDPR', href: '#gdpr' }
      ]
    }
  ];

  const socialLinks = [
    { icon: <Facebook />, href: '#facebook', label: 'Facebook' },
    { icon: <Twitter />, href: '#twitter', label: 'Twitter' },
    { icon: <LinkedIn />, href: '#linkedin', label: 'LinkedIn' },
    { icon: <Instagram />, href: '#instagram', label: 'Instagram' },
    { icon: <GitHub />, href: '#github', label: 'GitHub' }
  ];

  return (
    <Box sx={{ 
      mt: 'auto', 
      bgcolor: 'background.paper',
      borderTop: '1px solid',
      borderColor: 'divider'
    }}>
      <Container maxWidth="lg">
        {/* Main Footer Content */}
        <Box sx={{ py: 6 }}>
          <Grid container spacing={4}>
            {/* Brand Section */}
            <Grid item xs={12} md={4}>
              <Box sx={{ mb: 3 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                  <VideoCall sx={{ fontSize: 32, color: 'primary.main', mr: 1 }} />
                  <Typography 
                    variant="h5" 
                    fontWeight="bold"
                    sx={{
                      background: 'linear-gradient(45deg, #667eea 30%, #764ba2 90%)',
                      backgroundClip: 'text',
                      WebkitBackgroundClip: 'text',
                      WebkitTextFillColor: 'transparent'
                    }}
                  >
                    MeetPro
                  </Typography>
                </Box>
                
                <Typography variant="body2" color="text.secondary" sx={{ mb: 3, lineHeight: 1.6 }}>
                  Professional video meetings made simple. Connect, collaborate, and communicate 
                  with teams worldwide through our secure and reliable platform.
                </Typography>

                {/* Contact Info */}
                <Box sx={{ mb: 3 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                    <Email sx={{ fontSize: 16, color: 'text.secondary', mr: 1 }} />
                    <Typography variant="body2" color="text.secondary">
                      support@meetpro.com
                    </Typography>
                  </Box>
                  
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                    <Phone sx={{ fontSize: 16, color: 'text.secondary', mr: 1 }} />
                    <Typography variant="body2" color="text.secondary">
                      +1 (555) 123-4567
                    </Typography>
                  </Box>
                  
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <LocationOn sx={{ fontSize: 16, color: 'text.secondary', mr: 1 }} />
                    <Typography variant="body2" color="text.secondary">
                      San Francisco, CA
                    </Typography>
                  </Box>
                </Box>

                {/* Social Links */}
                <Box sx={{ display: 'flex', gap: 1 }}>
                  {socialLinks.map((social) => (
                    <Tooltip key={social.label} title={social.label}>
                      <IconButton
                        href={social.href}
                        size="small"
                        sx={{
                          color: 'text.secondary',
                          '&:hover': {
                            color: 'primary.main',
                            transform: 'translateY(-2px)'
                          }
                        }}
                      >
                        {social.icon}
                      </IconButton>
                    </Tooltip>
                  ))}
                </Box>
              </Box>
            </Grid>

            {/* Footer Links */}
            {footerSections.map((section) => (
              <Grid item xs={6} md={2} key={section.title}>
                <Typography 
                  variant="h6" 
                  fontWeight="bold" 
                  sx={{ mb: 2, fontSize: '1rem' }}
                >
                  {section.title}
                </Typography>
                
                <Box component="nav">
                  {section.links.map((link) => (
                    <Link
                      key={link.text}
                      href={link.href}
                      sx={{
                        display: 'block',
                        color: 'text.secondary',
                        textDecoration: 'none',
                        mb: 1,
                        fontSize: '0.875rem',
                        '&:hover': {
                          color: 'primary.main',
                          textDecoration: 'underline'
                        }
                      }}
                    >
                      {link.text}
                    </Link>
                  ))}
                </Box>
              </Grid>
            ))}
          </Grid>
        </Box>

        <Divider />

        {/* Bottom Footer */}
        <Box sx={{ 
          py: 3, 
          display: 'flex', 
          flexDirection: { xs: 'column', md: 'row' },
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: 2
        }}>
          <Typography variant="body2" color="text.secondary">
            © {currentYear} MeetPro. All rights reserved.
          </Typography>
          
          <Box sx={{ 
            display: 'flex', 
            gap: 3,
            flexWrap: 'wrap',
            justifyContent: { xs: 'center', md: 'flex-end' }
          }}>
            <Link 
              href="#privacy" 
              sx={{ 
                color: 'text.secondary', 
                textDecoration: 'none',
                fontSize: '0.875rem',
                '&:hover': { color: 'primary.main' }
              }}
            >
              Privacy Policy
            </Link>
            
            <Link 
              href="#terms" 
              sx={{ 
                color: 'text.secondary', 
                textDecoration: 'none',
                fontSize: '0.875rem',
                '&:hover': { color: 'primary.main' }
              }}
            >
              Terms of Service
            </Link>
            
            <Link 
              href="#cookies" 
              sx={{ 
                color: 'text.secondary', 
                textDecoration: 'none',
                fontSize: '0.875rem',
                '&:hover': { color: 'primary.main' }
              }}
            >
              Cookie Policy
            </Link>
            
            <Typography variant="body2" color="text.disabled">
              v2.1.0
            </Typography>
          </Box>
        </Box>
      </Container>
    </Box>
  );
};

export default Footer;