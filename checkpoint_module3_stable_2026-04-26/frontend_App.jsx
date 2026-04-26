import { useEffect, useMemo, useRef, useState } from 'react'
import axios from 'axios'

const STEPS = {
  HOME:       'home',
  INPUT:      'input',
  SKILLS:     'skills',
  ASSESSMENT: 'assessment',
  PLAN:       'plan',
}

const API_BASE = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000'
const REQUEST_TIMEOUT_MS = 30000
const startedAssessmentSessions = new Set()
const assessmentStartInFlight = new Map()

function formatCountdown(interviewDate) {
  if (!interviewDate) {
    return 'Set interview date to start timer'
  }

  const target = new Date(`${interviewDate}T23:59:59`)
  const now = new Date()
  const diff = target.getTime() - now.getTime()

  if (Number.isNaN(target.getTime())) {
    return 'Invalid interview date'
  }

  if (diff <= 0) {
    return 'Interview day reached'
  }

  const totalSeconds = Math.floor(diff / 1000)
  const days = Math.floor(totalSeconds / 86400)
  const hours = Math.floor((totalSeconds % 86400) / 3600)
  const mins = Math.floor((totalSeconds % 3600) / 60)
  const secs = totalSeconds % 60

  return `${days}d ${String(hours).padStart(2, '0')}h ${String(mins).padStart(2, '0')}m ${String(secs).padStart(2, '0')}s`
}

function HomePage({ onStart }) {
  const [hovered, setHovered] = useState(false)

  return (
    <div style={s.hero}>
      <div style={s.bgGlow} />
      <div style={s.heroInner}>
        <div style={s.badge}>✦ AI-Powered Career Prep</div>

        <h1 style={s.heroTitle}>
          Know exactly what <br />
          <span style={s.accentText}>to study</span> before your interview
        </h1>

        <p style={s.heroSub}>
          Upload your resume + paste a job description. Our AI assesses your
          real proficiency, finds the gaps, and builds a personalised study
          plan timed to your interview date.
        </p>

        <div style={s.stepPills}>
          {[
            { icon: '📄', label: 'Upload Resume & JD' },
            { icon: '🧠', label: 'AI Skill Assessment' },
            { icon: '📅', label: 'Personalised Study Plan' },
          ].map((item, i) => (
            <div key={i} style={s.pill}>
              <span style={s.pillIcon}>{item.icon}</span>
              <span>{item.label}</span>
            </div>
          ))}
        </div>

        <button
          style={{ ...s.ctaBtn, ...(hovered ? s.ctaBtnHover : {}) }}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
          onClick={onStart}
        >
          Get Started →
        </button>
      </div>
    </div>
  )
}

function InputPage({ onSubmitSuccess }) {
  const [resumeText, setResumeText] = useState('')
  const [jobDescription, setJobDescription] = useState('')
  const [interviewDate, setInterviewDate] = useState('')
  const [resumeFile, setResumeFile] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')
  const [countdown, setCountdown] = useState('Set interview date to start timer')

  useEffect(() => {
    setCountdown(formatCountdown(interviewDate))
    const id = setInterval(() => {
      setCountdown(formatCountdown(interviewDate))
    }, 1000)
    return () => clearInterval(id)
  }, [interviewDate])

  const daysRemainingText = useMemo(() => {
    if (!interviewDate) {
      return 'Days remaining: --'
    }
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    const target = new Date(`${interviewDate}T00:00:00`)
    if (Number.isNaN(target.getTime())) {
      return 'Days remaining: --'
    }
    const days = Math.floor((target.getTime() - today.getTime()) / (1000 * 60 * 60 * 24))
    return `Days remaining: ${days}`
  }, [interviewDate])

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')

    if (!jobDescription.trim()) {
      setError('Please add a job description.')
      return
    }

    if (!resumeText.trim() && !resumeFile) {
      setError('Please paste resume text or upload a resume PDF.')
      return
    }

    if (!interviewDate) {
      setError('Please select an interview date.')
      return
    }

    setIsLoading(true)
    try {
      const formData = new FormData()
      formData.append('resume_text', resumeText)
      formData.append('job_description', jobDescription)
      formData.append('interview_date', interviewDate)
      if (resumeFile) {
        formData.append('resume_file', resumeFile)
      }

      const response = await axios.post(`${API_BASE}/api/module2/extract-skills`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: REQUEST_TIMEOUT_MS,
      })

      onSubmitSuccess(response.data)
    } catch (err) {
      const message = err?.code === 'ECONNABORTED'
        ? 'Extract request timed out. Please ensure backend is running and retry.'
        : (err?.response?.data?.detail || 'Failed to extract skills. Please try again.')
      setError(message)
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <section style={s.pageWrap}>
      <div style={s.panelGlow} />
      <div style={s.formCard}>
        <h2 style={s.cardTitle}>Module 2: Input and Skill Extraction</h2>
        <p style={s.cardSub}>Upload resume PDF or paste text, add JD, pick interview date, and let AI split your skills.</p>

        <div style={s.timerStrip}>
          <div style={s.timerLabel}>Interview Countdown</div>
          <div style={s.timerValue}>{countdown}</div>
          <div style={s.daysValue}>{daysRemainingText}</div>
        </div>

        <form onSubmit={handleSubmit} style={s.formGrid}>
          <div style={s.fieldBlock}>
            <label style={s.label}>Resume Text (Optional if PDF uploaded)</label>
            <textarea
              style={s.textarea}
              rows={6}
              value={resumeText}
              onChange={(e) => setResumeText(e.target.value)}
              placeholder='Paste resume content here...'
            />
          </div>

          <div style={s.fieldBlock}>
            <label style={s.label}>Resume PDF Upload (Optional if text pasted)</label>
            <input
              style={s.inputFile}
              type='file'
              accept='application/pdf'
              onChange={(e) => setResumeFile(e.target.files?.[0] || null)}
            />
          </div>

          <div style={s.fieldBlock}>
            <label style={s.label}>Job Description</label>
            <textarea
              style={s.textarea}
              rows={8}
              value={jobDescription}
              onChange={(e) => setJobDescription(e.target.value)}
              placeholder='Paste full job description...'
            />
          </div>

          <div style={s.fieldBlock}>
            <label style={s.label}>Interview Date</label>
            <input
              style={s.input}
              type='date'
              value={interviewDate}
              onChange={(e) => setInterviewDate(e.target.value)}
            />
          </div>

          {error ? <div style={s.errorText}>{error}</div> : null}

          <button type='submit' style={s.submitBtn} disabled={isLoading}>
            {isLoading ? 'Extracting Skills...' : 'Extract Skills'}
          </button>
        </form>
      </div>
    </section>
  )
}

function SkillsPage({ skillResult, onBack, onStartAssessment }) {
  const [countdown, setCountdown] = useState('')
  const hasPresentSkills = (skillResult?.present_skills?.length || 0) > 0

  useEffect(() => {
    setCountdown(formatCountdown(skillResult?.interview_date))
    const id = setInterval(() => {
      setCountdown(formatCountdown(skillResult?.interview_date))
    }, 1000)
    return () => clearInterval(id)
  }, [skillResult])

  if (!skillResult) {
    return (
      <section style={s.pageWrap}>
        <div style={s.formCard}>
          <h2 style={s.cardTitle}>No skill extraction result yet</h2>
          <button style={s.submitBtn} onClick={onBack}>Back to Input</button>
        </div>
      </section>
    )
  }

  return (
    <section style={s.pageWrap}>
      <div style={s.panelGlow} />
      <div style={s.resultHead}>
        <h2 style={s.cardTitle}>Skill Split View</h2>
        <p style={s.cardSub}>Session #{skillResult.session_id} | Days Remaining: {skillResult.days_remaining}</p>
        <div style={s.timerValue}>{countdown}</div>
      </div>

      {!hasPresentSkills ? (
        <div style={s.infoBanner}>
          No matching present skills were found, so there is nothing to assess right now. You can move straight to the study plan for the lacking skills.
        </div>
      ) : null}

      <div style={s.splitGrid}>
        <div style={s.presentCard}>
          <h3 style={s.splitTitle}>Present Skills</h3>
          <p style={s.splitSub}>Found in both resume and job description</p>
          <div style={s.skillList}>
            {skillResult.present_skills.length ? (
              skillResult.present_skills.map((skill) => (
                <span key={skill} style={s.presentPill}>{skill}</span>
              ))
            ) : (
              <span style={s.emptyText}>No present skills found.</span>
            )}
          </div>
        </div>

        <div style={s.lackingCard}>
          <h3 style={s.splitTitle}>Lacking Skills</h3>
          <p style={s.splitSub}>Required in JD but missing in resume</p>
          <div style={s.skillList}>
            {skillResult.lacking_skills.length ? (
              skillResult.lacking_skills.map((skill) => (
                <span key={skill} style={s.lackingPill}>{skill}</span>
              ))
            ) : (
              <span style={s.emptyText}>No lacking skills found.</span>
            )}
          </div>
        </div>
      </div>

      <div style={s.resultActions}>
        <button style={s.primaryBtn} onClick={onStartAssessment}>
          {hasPresentSkills ? 'Start Assessment' : 'Get Study Plan'}
        </button>
        <button style={s.ghostBtn} onClick={onBack}>Edit Inputs</button>
      </div>
    </section>
  )
}

function AssessmentPage({ sessionId, skillResult, onBack }) {
  const [messages, setMessages] = useState([])
  const [currentQuestion, setCurrentQuestion] = useState(null)
  const [selectedOption, setSelectedOption] = useState('')
  const [summary, setSummary] = useState(null)
  const [isStarting, setIsStarting] = useState(true)
  const [isPreparingQuestions, setIsPreparingQuestions] = useState(false)
  const [isSending, setIsSending] = useState(false)
  const [error, setError] = useState('')
  const startedForSessionRef = useRef(null)
  const pollTimerRef = useRef(null)

  const appendAssistantQuestion = (question) => {
    setCurrentQuestion(question)
    setSelectedOption('')
    setMessages((prev) => [
      ...prev,
      {
        role: 'assistant',
        text: `(${question.skill_name} • Difficulty ${question.difficulty_level}/5) ${question.question_text}`,
      },
    ])
  }

  const loadNextQuestion = async () => {
    const response = await axios.get(`${API_BASE}/api/module3/sessions/${sessionId}/next-question`, {
      timeout: REQUEST_TIMEOUT_MS,
    })
    if (response.data.is_complete) {
      setCurrentQuestion(null)
      setSummary(response.data.summary)
      setMessages((prev) => [...prev, { role: 'assistant', text: 'Assessment complete. Summary ready below.' }])
      return
    }

    appendAssistantQuestion(response.data.next_question)
  }

  const stopPolling = () => {
    if (pollTimerRef.current) {
      clearTimeout(pollTimerRef.current)
      pollTimerRef.current = null
    }
  }

  const waitForQuestions = async (attempt = 0) => {
    try {
      const response = await axios.get(`${API_BASE}/api/module3/sessions/${sessionId}/next-question`, {
        timeout: REQUEST_TIMEOUT_MS,
      })

      if (response.data.is_complete) {
        stopPolling()
        setCurrentQuestion(null)
        setSummary(response.data.summary)
        setMessages((prev) => [...prev, { role: 'assistant', text: 'Assessment complete. Summary ready below.' }])
        setIsPreparingQuestions(false)
        return
      }

      if (response.data.next_question) {
        stopPolling()
        setIsPreparingQuestions(false)
        appendAssistantQuestion(response.data.next_question)
        return
      }

      if (attempt >= 20) {
        stopPolling()
        setIsPreparingQuestions(false)
        setError('Questions are still preparing. Please try Start Assessment again in a moment.')
        return
      }

      setIsPreparingQuestions(true)
      pollTimerRef.current = setTimeout(() => waitForQuestions(attempt + 1), 1500)
    } catch (err) {
      stopPolling()
      setIsPreparingQuestions(false)
      setError(err?.code === 'ECONNABORTED'
        ? 'Question preparation is taking too long. Please retry.'
        : (err?.response?.data?.detail || 'Failed to load questions.'))
    }
  }

  useEffect(() => {
    let active = true

    const bootstrap = async () => {
      if (startedForSessionRef.current === sessionId && messages.length > 0) {
        return
      }
      startedForSessionRef.current = sessionId
      setIsStarting(true)
      setError('')
      try {
        let startResponse = null
        if (!startedAssessmentSessions.has(sessionId)) {
          if (!assessmentStartInFlight.has(sessionId)) {
            const startPromise = axios.post(`${API_BASE}/api/module3/sessions/${sessionId}/start-assessment`, null, {
              timeout: REQUEST_TIMEOUT_MS,
            })
            assessmentStartInFlight.set(sessionId, startPromise)
          }

          startResponse = await assessmentStartInFlight.get(sessionId)
          startedAssessmentSessions.add(sessionId)
          assessmentStartInFlight.delete(sessionId)
        } else {
          startResponse = await axios.post(`${API_BASE}/api/module3/sessions/${sessionId}/start-assessment`, null, {
            timeout: REQUEST_TIMEOUT_MS,
          })
        }
        if (!active) return
        setMessages([
          {
            role: 'assistant',
            text: 'Assessment started. Choose one option for each question and I will score it instantly.',
          },
        ])

        if (!active) return
        if (startResponse.data?.next_question) {
          appendAssistantQuestion(startResponse.data.next_question)
        } else {
          await waitForQuestions()
        }
      } catch (err) {
        if (!active) return
        assessmentStartInFlight.delete(sessionId)
        startedForSessionRef.current = null
        const message = err?.code === 'ECONNABORTED'
          ? 'Starting assessment timed out. Please retry.'
          : (err?.response?.data?.detail || 'Failed to start assessment.')
        setError(message)
      } finally {
        if (active) setIsStarting(false)
      }
    }

    bootstrap()
    return () => {
      active = false
      stopPolling()
    }
  }, [sessionId])

  const handleSend = async (e) => {
    e.preventDefault()
    if (isSending || !selectedOption || !currentQuestion) return

    setIsSending(true)
    setError('')
    const selectedText = currentQuestion.options[Number(selectedOption) - 1]
    setMessages((prev) => [...prev, { role: 'user', text: selectedText }])

    try {
      const formData = new FormData()
      formData.append('selected_option_index', selectedOption)
      const response = await axios.post(
        `${API_BASE}/api/module3/questions/${currentQuestion.id}/answer`,
        formData,
        {
          headers: { 'Content-Type': 'multipart/form-data' },
          timeout: REQUEST_TIMEOUT_MS,
        },
      )

      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          text: `Score: ${response.data.score} | ${response.data.feedback}`,
          meta: response.data.avg_skill_score != null
            ? `Average ${response.data.skill_name}: ${response.data.avg_skill_score} | Priority weight: ${response.data.priority_weight}`
            : '',
        },
      ])

      if (response.data.is_complete) {
        setCurrentQuestion(null)
        setSummary(response.data.summary)
        setMessages((prev) => [...prev, { role: 'assistant', text: 'All questions are complete. Summary generated.' }])
      } else if (response.data.next_question) {
        appendAssistantQuestion(response.data.next_question)
      } else {
        await loadNextQuestion()
      }
    } catch (err) {
      const message = err?.code === 'ECONNABORTED'
        ? 'Answer validation timed out. Please submit once more.'
        : (err?.response?.data?.detail || 'Failed to score answer.')
      setError(message)
    } finally {
      setIsSending(false)
    }
  }

  return (
    <section style={s.pageWrap}>
      <div style={s.panelGlow} />
      <div style={s.assessHeader}>
        <h2 style={s.cardTitle}>Module 3: Proficiency Assessment</h2>
        <p style={s.cardSub}>
          Conversational MCQ scoring for {skillResult?.present_skills?.length || 0} present skills.
        </p>
      </div>

      <div style={s.chatShell}>
        <div style={s.chatSidebar}>
          <div style={s.sidebarTitle}>Assessment Focus</div>
          <div style={s.sidebarStat}>{skillResult?.present_skills?.length || 0} present skills</div>
          <div style={s.sidebarStat}>{skillResult?.lacking_skills?.length || 0} lacking skills</div>
          <div style={s.sidebarStat}>Session #{sessionId}</div>
          <button style={s.secondaryBtn} onClick={onBack}>Back to Skills</button>
        </div>

        <div style={s.chatWindow}>
          <div style={s.chatMessages}>
            {messages.map((message, index) => (
              <div
                key={index}
                style={{
                  ...s.chatBubble,
                  ...(message.role === 'user' ? s.userBubble : s.assistantBubble),
                }}
              >
                <div style={message.role === 'assistant' ? { whiteSpace: 'pre-wrap' } : undefined}>{message.text}</div>
                {message.meta ? <div style={s.bubbleMeta}>{message.meta}</div> : null}
              </div>
            ))}
            {isStarting ? <div style={s.typingText}>Starting assessment...</div> : null}
            {isPreparingQuestions ? <div style={s.typingText}>Preparing AI questions...</div> : null}
          </div>

          {currentQuestion ? (
            <form onSubmit={handleSend} style={s.optionPanel}>
              <div style={s.optionList}>
                {currentQuestion.options.map((option, index) => {
                  const value = String(index + 1)
                  const isSelected = selectedOption === value
                  return (
                    <button
                      key={value}
                      type="button"
                      style={{
                        ...s.optionBtn,
                        ...(isSelected ? s.optionBtnSelected : {}),
                      }}
                      onClick={() => setSelectedOption(value)}
                    >
                      <div style={s.optionLetter}>{String.fromCharCode(65 + index)}</div>
                      <div style={s.optionText}>{option}</div>
                    </button>
                  )
                })}
              </div>

              <button style={s.sendBtn} type='submit' disabled={!selectedOption || isSending}>
                {isSending ? 'Scoring...' : 'Submit Option'}
              </button>
            </form>
          ) : null}

          {summary ? (
            <div style={s.summaryPanel}>
              <div style={s.summaryTitle}>Assessment Summary</div>
              <div style={s.summaryRow}>Progress: {summary.progress_percent}%</div>
              <div style={s.summaryRow}>Answered: {summary.answered_questions} / {summary.total_questions}</div>
              <div style={s.summaryGrid}>
                {summary.skills.map((skill) => (
                  <div key={skill.id} style={s.skillScoreCard}>
                    <div style={s.skillScoreName}>{skill.skill_name}</div>
                    <div style={s.skillScoreMeta}>Score: {skill.proficiency_score ?? 'n/a'}</div>
                    <div style={s.skillScoreMeta}>Priority: {skill.priority_weight ?? 'n/a'}</div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          {error ? <div style={s.errorText}>{error}</div> : null}
        </div>
      </div>
    </section>
  )
}

function PlaceholderPage({ title, description }) {
  return (
    <div style={s.placeholder}>
      <div style={{ fontSize: 52, marginBottom: 8 }}>🚧</div>
      <h2 style={s.placeholderTitle}>{title}</h2>
      <p style={s.placeholderDesc}>{description}</p>
    </div>
  )
}

function Nav({ step, onHome }) {
  const navStepList = [
    { label: 'Input',      key: STEPS.INPUT },
    { label: 'Skills',     key: STEPS.SKILLS },
    { label: 'Assessment', key: STEPS.ASSESSMENT },
    { label: 'Plan',       key: STEPS.PLAN },
  ]
  const currentIndex = navStepList.findIndex(n => n.key === step)

  return (
    <nav style={s.nav}>
      <button style={s.logo} onClick={onHome}>
        <span style={s.logoIcon}>◈</span> SkillBridge
      </button>

      {step !== STEPS.HOME && (
        <div style={s.navSteps}>
          {navStepList.map((item, i) => {
            const isActive = step === item.key
            const isDone   = currentIndex > i
            return (
              <div
                key={item.key}
                style={{
                  ...s.navStep,
                  ...(isActive ? s.navStepActive : {}),
                  ...(isDone   ? s.navStepDone   : {}),
                }}
              >
                <span style={{
                  ...s.navNum,
                  ...(isActive ? s.navNumActive : {}),
                  ...(isDone   ? s.navNumDone   : {}),
                }}>
                  {isDone ? '✓' : i + 1}
                </span>
                {item.label}
              </div>
            )
          })}
        </div>
      )}
    </nav>
  )
}

export default function App() {
  const [step, setStep]           = useState(STEPS.HOME)
  const [sessionId, setSessionId] = useState(null)
  const [skillResult, setSkillResult] = useState(null)

  return (
    <div style={s.app}>
      <Nav step={step} onHome={() => setStep(STEPS.HOME)} />
      <main style={s.main}>
        {step === STEPS.HOME && (
          <HomePage onStart={() => setStep(STEPS.INPUT)} />
        )}
        {step === STEPS.INPUT && (
          <InputPage
            onSubmitSuccess={(data) => {
              setSessionId(data.session_id)
              setSkillResult(data)
              setStep(STEPS.SKILLS)
            }}
          />
        )}
        {step === STEPS.SKILLS && (
          <SkillsPage
            skillResult={skillResult}
            onBack={() => setStep(STEPS.INPUT)}
            onStartAssessment={() => {
              const hasPresentSkills = (skillResult?.present_skills?.length || 0) > 0
              setStep(hasPresentSkills ? STEPS.ASSESSMENT : STEPS.PLAN)
            }}
          />
        )}
        {step === STEPS.ASSESSMENT && (
          <AssessmentPage
            sessionId={sessionId}
            skillResult={skillResult}
            onBack={() => setStep(STEPS.SKILLS)}
          />
        )}
        {step === STEPS.PLAN && (
          <PlaceholderPage
            title="Step 4 — Your Study Plan"
            description="A personalised, time-boxed plan with resources and checkboxes."
          />
        )}
      </main>
    </div>
  )
}

const s = {
  app: {
    minHeight: '100vh',
    display: 'flex',
    flexDirection: 'column',
    background: '#0a0a0f',
    color: '#f0f0f8',
    fontFamily: "'DM Sans', sans-serif",
  },
  nav: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '0 40px',
    height: 64,
    borderBottom: '1px solid #1e1e2e',
    background: 'rgba(10,10,15,0.85)',
    backdropFilter: 'blur(12px)',
    position: 'sticky',
    top: 0,
    zIndex: 100,
  },
  logo: {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    fontFamily: "'Syne', sans-serif",
    fontSize: 18,
    fontWeight: 700,
    color: '#f0f0f8',
    letterSpacing: '-0.3px',
  },
  logoIcon: { fontSize: 20, color: '#6c63ff' },
  navSteps: { display: 'flex', alignItems: 'center', gap: 4 },
  navStep: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '5px 14px',
    borderRadius: 99,
    fontSize: 13,
    color: '#55556a',
    fontFamily: "'Syne', sans-serif",
    fontWeight: 600,
  },
  navStepActive: { color: '#6c63ff', background: 'rgba(108,99,255,0.1)' },
  navStepDone:   { color: '#43e97b' },
  navNum: {
    width: 20, height: 20,
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    borderRadius: '50%', fontSize: 11,
    background: '#2a2a3a', color: '#8888aa', fontWeight: 700,
  },
  navNumActive: { background: '#6c63ff', color: '#fff' },
  navNumDone:   { background: '#43e97b', color: '#0a0a0f' },
  main: { flex: 1, display: 'flex', flexDirection: 'column' },
  hero: {
    flex: 1,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '80px 40px',
    position: 'relative',
    overflow: 'hidden',
    minHeight: 'calc(100vh - 64px)',
  },
  bgGlow: {
    position: 'absolute',
    top: '20%', left: '50%',
    transform: 'translateX(-50%)',
    width: 700, height: 700,
    borderRadius: '50%',
    background: 'radial-gradient(circle, rgba(108,99,255,0.09) 0%, transparent 70%)',
    pointerEvents: 'none',
  },
  heroInner: {
    maxWidth: 640, textAlign: 'center',
    position: 'relative', zIndex: 1,
    display: 'flex', flexDirection: 'column', alignItems: 'center',
  },
  badge: {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '5px 16px',
    borderRadius: 99,
    border: '1px solid #2a2a3a',
    fontSize: 11,
    fontFamily: "'Syne', sans-serif",
    fontWeight: 700,
    color: '#6c63ff',
    letterSpacing: '1px',
    textTransform: 'uppercase',
    marginBottom: 28,
    background: 'rgba(108,99,255,0.07)',
  },
  heroTitle: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 'clamp(36px, 5.5vw, 56px)',
    fontWeight: 800,
    lineHeight: 1.1,
    letterSpacing: '-1.5px',
    color: '#f0f0f8',
    marginBottom: 20,
  },
  accentText: { color: '#6c63ff' },
  heroSub: {
    fontSize: 16, color: '#8888aa', lineHeight: 1.75,
    maxWidth: 480, marginBottom: 36, fontWeight: 300,
  },
  stepPills: {
    display: 'flex', justifyContent: 'center',
    gap: 10, marginBottom: 44, flexWrap: 'wrap',
  },
  pill: {
    display: 'flex', alignItems: 'center', gap: 8,
    padding: '10px 18px',
    background: '#111118', border: '1px solid #2a2a3a',
    borderRadius: 12, fontSize: 13, color: '#8888aa', fontWeight: 500,
  },
  pillIcon: { fontSize: 16 },
  ctaBtn: {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '14px 40px',
    background: '#6c63ff',
    color: '#fff', border: 'none', borderRadius: 12,
    fontFamily: "'Syne', sans-serif",
    fontSize: 16, fontWeight: 700, cursor: 'pointer',
    boxShadow: '0 0 48px rgba(108,99,255,0.3)',
    letterSpacing: '0.3px',
    transition: 'transform 0.2s ease, box-shadow 0.2s ease',
  },
  ctaBtnHover: {
    transform: 'translateY(-2px)',
    boxShadow: '0 0 64px rgba(108,99,255,0.45)',
  },
  pageWrap: {
    position: 'relative',
    minHeight: 'calc(100vh - 64px)',
    padding: '34px clamp(18px, 4vw, 42px)',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    gap: 18,
  },
  panelGlow: {
    position: 'absolute',
    inset: 0,
    background: 'radial-gradient(60% 55% at 10% 10%, rgba(108,99,255,0.16), transparent 70%)',
    pointerEvents: 'none',
  },
  formCard: {
    width: 'min(980px, 100%)',
    background: 'linear-gradient(160deg, #10101a, #0e0e16)',
    border: '1px solid #2a2a3a',
    borderRadius: 18,
    padding: '24px clamp(16px, 3vw, 28px)',
    boxShadow: '0 20px 60px rgba(0,0,0,0.3)',
    position: 'relative',
    zIndex: 1,
  },
  cardTitle: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 'clamp(22px, 3vw, 30px)',
    marginBottom: 8,
    color: '#f0f0f8',
  },
  cardSub: {
    color: '#8f8fae',
    marginBottom: 16,
    fontSize: 14,
  },
  timerStrip: {
    border: '1px solid #2f2f48',
    borderRadius: 14,
    padding: '12px 14px',
    marginBottom: 16,
    background: 'rgba(108,99,255,0.08)',
  },
  timerLabel: {
    fontSize: 12,
    color: '#a5a5d2',
    marginBottom: 4,
    textTransform: 'uppercase',
    letterSpacing: '0.8px',
  },
  timerValue: {
    fontFamily: "'Syne', sans-serif",
    color: '#f8f8ff',
    fontSize: 'clamp(18px, 3vw, 24px)',
    lineHeight: 1.2,
  },
  daysValue: {
    marginTop: 4,
    color: '#9e9ebc',
    fontSize: 13,
  },
  formGrid: {
    display: 'grid',
    gridTemplateColumns: '1fr',
    gap: 14,
  },
  fieldBlock: {
    display: 'flex',
    flexDirection: 'column',
    gap: 6,
  },
  label: {
    color: '#ddddef',
    fontSize: 13,
    fontWeight: 600,
    letterSpacing: '0.3px',
  },
  textarea: {
    width: '100%',
    resize: 'vertical',
    background: '#0a0a12',
    color: '#f0f0f8',
    border: '1px solid #2b2b3d',
    borderRadius: 12,
    padding: '12px 14px',
    fontFamily: "'DM Sans', sans-serif",
    fontSize: 14,
    lineHeight: 1.6,
    outline: 'none',
  },
  input: {
    background: '#0a0a12',
    color: '#f0f0f8',
    border: '1px solid #2b2b3d',
    borderRadius: 12,
    padding: '10px 12px',
    fontFamily: "'DM Sans', sans-serif",
    fontSize: 14,
  },
  inputFile: {
    background: '#11111a',
    color: '#e2e2fa',
    border: '1px dashed #3f3f58',
    borderRadius: 12,
    padding: '10px 12px',
  },
  submitBtn: {
    marginTop: 4,
    border: 'none',
    borderRadius: 12,
    padding: '12px 18px',
    background: '#6c63ff',
    color: '#fff',
    fontFamily: "'Syne', sans-serif",
    fontWeight: 700,
    cursor: 'pointer',
    width: 'fit-content',
  },
  errorText: {
    color: '#ff9090',
    background: 'rgba(255,100,100,0.08)',
    border: '1px solid rgba(255,120,120,0.25)',
    borderRadius: 10,
    padding: '8px 10px',
    fontSize: 13,
  },
  resultHead: {
    width: 'min(980px, 100%)',
    position: 'relative',
    zIndex: 1,
    marginBottom: 4,
  },
  splitGrid: {
    width: 'min(980px, 100%)',
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
    gap: 16,
    position: 'relative',
    zIndex: 1,
  },
  presentCard: {
    background: 'linear-gradient(180deg, #0f1913, #0b1210)',
    border: '1px solid #255637',
    borderRadius: 16,
    padding: 16,
  },
  lackingCard: {
    background: 'linear-gradient(180deg, #1b1212, #140d0d)',
    border: '1px solid #6a2f2f',
    borderRadius: 16,
    padding: 16,
  },
  splitTitle: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 20,
    marginBottom: 2,
  },
  splitSub: {
    color: '#a9a9bc',
    marginBottom: 12,
    fontSize: 13,
  },
  skillList: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 8,
  },
  presentPill: {
    borderRadius: 999,
    padding: '7px 12px',
    fontSize: 13,
    fontWeight: 600,
    border: '1px solid #2f7d4f',
    background: 'rgba(62, 181, 108, 0.16)',
    color: '#9bffc6',
  },
  lackingPill: {
    borderRadius: 999,
    padding: '7px 12px',
    fontSize: 13,
    fontWeight: 600,
    border: '1px solid #a04a4a',
    background: 'rgba(235, 97, 97, 0.14)',
    color: '#ffc1c1',
  },
  emptyText: {
    color: '#b2b2c5',
    fontSize: 14,
  },
  resultActions: {
    width: 'min(980px, 100%)',
    position: 'relative',
    zIndex: 1,
    display: 'flex',
    justifyContent: 'flex-start',
    marginTop: 6,
    gap: 12,
  },
  infoBanner: {
    width: 'min(980px, 100%)',
    position: 'relative',
    zIndex: 1,
    borderRadius: 14,
    border: '1px solid #5c4b1f',
    background: 'rgba(255, 193, 7, 0.08)',
    color: '#f4ddb0',
    padding: '12px 14px',
    fontSize: 14,
    lineHeight: 1.6,
  },
  primaryBtn: {
    borderRadius: 12,
    border: '1px solid #6c63ff',
    background: '#6c63ff',
    color: '#fff',
    padding: '10px 14px',
    cursor: 'pointer',
    fontFamily: "'Syne', sans-serif",
    fontWeight: 700,
  },
  ghostBtn: {
    borderRadius: 12,
    border: '1px solid #36364f',
    background: 'transparent',
    color: '#d7d7f0',
    padding: '10px 14px',
    cursor: 'pointer',
    fontFamily: "'Syne', sans-serif",
    fontWeight: 600,
  },
  assessHeader: {
    width: 'min(1120px, 100%)',
    position: 'relative',
    zIndex: 1,
  },
  chatShell: {
    width: 'min(1120px, 100%)',
    display: 'grid',
    gridTemplateColumns: '280px 1fr',
    gap: 16,
    position: 'relative',
    zIndex: 1,
  },
  chatSidebar: {
    background: 'linear-gradient(180deg, #12121b, #0d0d14)',
    border: '1px solid #2a2a3a',
    borderRadius: 18,
    padding: 18,
    height: 'fit-content',
  },
  sidebarTitle: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 18,
    marginBottom: 12,
    color: '#f0f0f8',
  },
  sidebarStat: {
    background: '#11111a',
    border: '1px solid #2b2b3d',
    color: '#cfcfe6',
    borderRadius: 12,
    padding: '10px 12px',
    marginBottom: 10,
    fontSize: 13,
  },
  secondaryBtn: {
    width: '100%',
    marginTop: 8,
    borderRadius: 12,
    border: '1px solid #36364f',
    background: 'transparent',
    color: '#d7d7f0',
    padding: '10px 14px',
    cursor: 'pointer',
    fontFamily: "'Syne', sans-serif",
    fontWeight: 600,
  },
  chatWindow: {
    background: 'linear-gradient(180deg, #10101a, #0d0d14)',
    border: '1px solid #2a2a3a',
    borderRadius: 18,
    padding: 18,
    display: 'flex',
    flexDirection: 'column',
    gap: 14,
    minHeight: 520,
  },
  chatMessages: {
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
    flex: 1,
    overflowY: 'auto',
    paddingRight: 4,
  },
  chatBubble: {
    maxWidth: '85%',
    padding: '12px 14px',
    borderRadius: 16,
    lineHeight: 1.6,
    fontSize: 14,
  },
  assistantBubble: {
    background: '#171726',
    border: '1px solid #2f2f48',
    color: '#f1f1ff',
    alignSelf: 'flex-start',
  },
  userBubble: {
    background: 'rgba(108,99,255,0.16)',
    border: '1px solid rgba(108,99,255,0.4)',
    color: '#fff',
    alignSelf: 'flex-end',
  },
  bubbleMeta: {
    marginTop: 6,
    fontSize: 12,
    color: '#9f9fbe',
  },
  typingText: {
    color: '#8f8fae',
    fontSize: 13,
    fontStyle: 'italic',
  },
  chatComposer: {
    display: 'flex',
    gap: 10,
    alignItems: 'flex-end',
  },
  optionPanel: {
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
  },
  optionList: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
    gap: 10,
  },
  optionBtn: {
    textAlign: 'left',
    display: 'flex',
    gap: 10,
    alignItems: 'flex-start',
    padding: '12px 14px',
    borderRadius: 14,
    border: '1px solid #2b2b3d',
    background: '#0a0a12',
    color: '#f0f0f8',
    cursor: 'pointer',
  },
  optionBtnSelected: {
    border: '1px solid #6c63ff',
    background: 'rgba(108,99,255,0.1)',
  },
  optionLetter: {
    width: 28,
    height: 28,
    borderRadius: '50%',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    background: '#1c1c2a',
    color: '#fff',
    fontFamily: "'Syne', sans-serif",
    fontWeight: 700,
    flexShrink: 0,
  },
  optionText: {
    margin: 0,
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
    fontFamily: "'DM Sans', sans-serif",
    fontSize: 13,
    lineHeight: 1.55,
    color: '#e8e8fb',
  },
  chatInput: {
    flex: 1,
    resize: 'vertical',
    background: '#0a0a12',
    color: '#f0f0f8',
    border: '1px solid #2b2b3d',
    borderRadius: 14,
    padding: '12px 14px',
    fontFamily: "'DM Sans', sans-serif",
    fontSize: 14,
    lineHeight: 1.6,
    outline: 'none',
  },
  sendBtn: {
    border: 'none',
    borderRadius: 14,
    padding: '12px 18px',
    background: '#6c63ff',
    color: '#fff',
    fontFamily: "'Syne', sans-serif",
    fontWeight: 700,
    cursor: 'pointer',
    minWidth: 140,
  },
  summaryPanel: {
    background: '#11111a',
    border: '1px solid #2b2b3d',
    borderRadius: 16,
    padding: 14,
  },
  summaryTitle: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 18,
    marginBottom: 8,
  },
  summaryRow: {
    color: '#cfcfe6',
    fontSize: 13,
    marginBottom: 4,
  },
  summaryGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
    gap: 10,
    marginTop: 12,
  },
  skillScoreCard: {
    borderRadius: 12,
    padding: 12,
    background: '#0d0d14',
    border: '1px solid #2b2b3d',
  },
  skillScoreName: {
    fontWeight: 700,
    color: '#f0f0f8',
    marginBottom: 4,
  },
  skillScoreMeta: {
    color: '#a6a6c4',
    fontSize: 12,
  },
  placeholder: {
    flex: 1, display: 'flex', flexDirection: 'column',
    alignItems: 'center', justifyContent: 'center',
    padding: 60, textAlign: 'center', gap: 12,
    minHeight: 'calc(100vh - 64px)',
  },
  placeholderTitle: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 22, fontWeight: 700, color: '#f0f0f8',
  },
  placeholderDesc: { color: '#8888aa', maxWidth: 420, lineHeight: 1.7 },
}
